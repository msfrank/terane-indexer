package com.syntaxjockey.terane.indexer.sink

import scala.concurrent.Future
import scala.collection.JavaConversions._
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.event.{ActorEventBus, SubchannelClassification}
import akka.util.Subclassification
import akka.pattern.pipe
import org.joda.time.{DateTimeZone, DateTime}
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import java.util.UUID

import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldBus
import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.UUIDLike
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.ddl.SchemaChangeResult
import akka.actor.Status.Failure
import com.syntaxjockey.terane.indexer.cassandra.CassandraCFOperations
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperClient

/**
 *
 *  + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      + "fields"
 *        + fieldTypeAndName: String -> id: UUIDLike
 *          - "created" -> Long
 *          - "count" -> UUID
 *          - "frequency" -> UUID
 *
 */
class FieldManager(store: Store, val keyspace: Keyspace, zk: ZookeeperClient, fieldBus: FieldBus) extends Actor with ActorLogging with CassandraCFOperations {
  import FieldManager._
  import UUIDLike._

  import context.dispatcher

  val shardingFactor = 3
  var currentFields = FieldsChanged(Map.empty, Map.empty)
  var creatingFields: Set[FieldIdentifier] = Set.empty
  var changingFields: Set[FieldIdentifier] = Set.empty
  var removingFields: Set[FieldIdentifier] = Set.empty

  getFields pipeTo self

  log.debug("started {}", self.path.name)

  def receive = {

    /* notify all subscribers that fields have changed */
    case fieldsChanged: FieldsChanged =>
      log.debug("fields have changed")
      currentFields = fieldsChanged
      fieldBus.publish(currentFields)

    /* send current fields to sender */
    case GetFields =>
      sender ! currentFields

    /* create a new field */
    case CreateField(fieldId) if !creatingFields.contains(fieldId) =>
      creatingFields = creatingFields + fieldId
      log.debug("creating field {}", fieldId)
      createField(fieldId) pipeTo self

    /* a new field was created */
    case CreatedField(fieldId, field, fcf, schemaChangeId) =>
      val fieldsByIdent = currentFields.fieldsByIdent
      val fieldsByCf = currentFields.fieldsByCf
      currentFields = FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
      creatingFields = creatingFields - fieldId
      fieldBus.publish(currentFields)

    case Failure(cause) =>
      log.debug("received failure: {}", cause.getMessage)
  }

  /**
   * Asynchronously retrieve the list of fields.
   *
   * @return
   */
  def getFields = Future[FieldsChanged] {
    val basepath = "/stores/" + store.name + "/fields"
    val znodes = zk.client.getChildren.forPath(basepath)
    log.debug("found {} fields in {}", znodes.length, basepath)
    znodes.foldLeft(FieldsChanged(Map.empty, Map.empty)) {
      (fieldsChanged, fieldNode) =>
      val FieldsChanged(fieldsByIdent, fieldsByCf) = fieldsChanged
      val fieldPath = basepath + "/" + fieldNode
      val id = new String(zk.client.getData.forPath(fieldPath), ZookeeperClient.UTF_8_CHARSET)
      val fieldNodeParts = fieldNode.split(":", 2)
      val fieldType = EventValueType.withName(fieldNodeParts(0))
      val fieldName = fieldNodeParts(1)
      val createdString = new String(zk.client.getData.forPath(fieldPath + "/created"), ZookeeperClient.UTF_8_CHARSET)
      val created = new DateTime(createdString.toLong, DateTimeZone.UTC)
      val fieldId = FieldIdentifier(fieldName, fieldType)
      if (fieldsByIdent.contains(fieldId))
        throw new Exception("field %s:%s already exists".format(fieldType.toString, fieldName))
      /* create the new field column family and update the field maps */
      val _fieldsChanged = fieldType match {
        case EventValueType.TEXT =>
          val fcf = new TypedFieldColumnFamily(fieldName, id, shardingFactor, new TextField(),
            new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Text))
          val field = Field(fieldId, created, text = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case EventValueType.LITERAL =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new LiteralField(),
            new ColumnFamily[java.lang.Long,StringPosting](id.toString, LongSerializer.get, FieldSerializers.Literal))
          val field = Field(fieldId, created, literal = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case EventValueType.INTEGER =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new IntegerField(),
            new ColumnFamily[java.lang.Long,LongPosting](id.toString, LongSerializer.get, FieldSerializers.Integer))
          val field = Field(fieldId, created, integer = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case EventValueType.FLOAT =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new FloatField(),
            new ColumnFamily[java.lang.Long,DoublePosting](id.toString, LongSerializer.get, FieldSerializers.Float))
          val field = Field(fieldId, created, float = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case EventValueType.DATETIME =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new DatetimeField(),
            new ColumnFamily[java.lang.Long,DatePosting](id.toString, LongSerializer.get, FieldSerializers.Datetime))
          val field = Field(fieldId, created, datetime = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case EventValueType.ADDRESS =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new AddressField(),
            new ColumnFamily[java.lang.Long,AddressPosting](id.toString, LongSerializer.get, FieldSerializers.Address))
          val field = Field(fieldId, created, address = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case EventValueType.HOSTNAME =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new HostnameField(),
            new ColumnFamily[java.lang.Long,StringPosting](id.toString, LongSerializer.get, FieldSerializers.Hostname))
          val field = Field(fieldId, created, hostname = Some(fcf))
          FieldsChanged(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
      }
      log.debug("found field {}:{} with id {}", fieldType.toString, fieldName, id)
      _fieldsChanged
    }
  }

  /**
   * Asynchronously create a new field.
   *
   * @param fieldId
   * @return
   */
  def createField(fieldId: FieldIdentifier) = Future[CreatedField] {
    val path = "/stores/" + store.name + "/fields/" + fieldId.fieldType.toString + ":" + fieldId.fieldName
    val id: UUIDLike = UUID.randomUUID()
    val created = DateTime.now(DateTimeZone.UTC)
    /* lock field */

    /* check whether field already exists */

    /* create the column family in cassandra */
    val createdField = fieldId.fieldType match {
      case EventValueType.TEXT =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new TextField(),
          new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Text))
        val field = Field(fieldId, created, text = Some(fcf))
        CreatedField(fieldId, field, fcf, createTextField(id).getResult)
      case EventValueType.LITERAL =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new LiteralField(),
          new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Literal))
        val field = Field(fieldId, created, literal = Some(fcf))
        CreatedField(fieldId, field, fcf, createLiteralField(id).getResult)
      case EventValueType.INTEGER =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new IntegerField(),
          new ColumnFamily[java.lang.Long,LongPosting](id, LongSerializer.get, FieldSerializers.Integer))
        val field = Field(fieldId, created, integer = Some(fcf))
        CreatedField(fieldId, field, fcf, createIntegerField(id).getResult)
      case EventValueType.FLOAT =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new FloatField(),
          new ColumnFamily[java.lang.Long,DoublePosting](id, LongSerializer.get, FieldSerializers.Float))
        val field = Field(fieldId, created, float = Some(fcf))
        CreatedField(fieldId, field, fcf, createFloatField(id).getResult)
      case EventValueType.DATETIME =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new DatetimeField(),
          new ColumnFamily[java.lang.Long,DatePosting](id, LongSerializer.get, FieldSerializers.Datetime))
        val field = Field(fieldId, created, datetime = Some(fcf))
        CreatedField(fieldId, field, fcf, createDatetimeField(id).getResult)
      case EventValueType.ADDRESS =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new AddressField(),
          new ColumnFamily[java.lang.Long,AddressPosting](id, LongSerializer.get, FieldSerializers.Address))
        val field = Field(fieldId, created, address = Some(fcf))
        CreatedField(fieldId, field, fcf, createAddressField(id).getResult)
      case EventValueType.HOSTNAME =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new HostnameField(),
          new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Hostname))
        val field = Field(fieldId, created, hostname = Some(fcf))
        CreatedField(fieldId, field, fcf, createHostnameField(id).getResult)
    }
    /* create the field in zookeeper */
    zk.client.inTransaction()
      .create().forPath(path, id.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
      .and()
      .create().forPath(path + "/created", created.getMillis.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
      .and()
      .commit()
    /* unlock field */
    log.debug("created field {}:{} in store {} (schema change id is {})",
      fieldId.fieldName, fieldId.fieldType.toString, store.name, createdField.schemaChangeId.getSchemaId)
    createdField
  }
}

object FieldManager {

  case class FieldColumnFamily(name: String, id: String, width: Long)
  class TypedFieldColumnFamily[F,P](
    override val name: String,
    override val id: String,
    override val width: Long,
    val field: F,
    val cf: ColumnFamily[java.lang.Long,P]) extends FieldColumnFamily(name, id, width)

  case class Field(
    fieldId: FieldIdentifier, created: DateTime,
    text: Option[TypedFieldColumnFamily[TextField,StringPosting]] = None,
    literal: Option[TypedFieldColumnFamily[LiteralField,StringPosting]] = None,
    integer: Option[TypedFieldColumnFamily[IntegerField,LongPosting]] = None,
    float: Option[TypedFieldColumnFamily[FloatField,DoublePosting]] = None,
    datetime: Option[TypedFieldColumnFamily[DatetimeField,DatePosting]] = None,
    address: Option[TypedFieldColumnFamily[AddressField,AddressPosting]] = None,
    hostname: Option[TypedFieldColumnFamily[HostnameField,StringPosting]] = None)

  case object GetFields
  case class CreateField(fieldId: FieldIdentifier)
  case class CreatedField(fieldId: FieldIdentifier, field: Field, fcf: FieldColumnFamily, schemaChangeId: SchemaChangeResult)
  case class DeleteField(field: FieldIdentifier)
  case class DeletedField(fieldId: FieldIdentifier, field: Field, schemaChangeId: SchemaChangeResult)

  class FieldBus extends ActorEventBus with SubchannelClassification {
    type Event = FieldEvent
    type Classifier = Class[_]

    protected implicit val subclassification = new Subclassification[Class[_]] {
      def isEqual(x: Class[_], y: Class[_]) = x == y
      def isSubclass(x: Class[_], y: Class[_]) = y isAssignableFrom x
    }

    protected def classify(event: FieldEvent): Class[_] = event.getClass

    protected def publish(event: FieldEvent, subscriber: ActorRef) { subscriber ! event }
  }

  sealed trait FieldEvent

  sealed trait FieldNotification extends FieldEvent
  case class FieldsChanged(fieldsByIdent: Map[FieldIdentifier,Field], fieldsByCf: Map[String,Field]) extends FieldNotification
}
