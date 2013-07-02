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

import com.syntaxjockey.terane.indexer.metadata.ZookeeperClient
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldBus
import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.UUIDLike
import com.netflix.astyanax.Keyspace

/**
 *
 *  + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      + "fields"
 *        + field: String -> id: UUIDLike
 *          - "type" -> String
 *          - "created" -> Long
 *          - "count" -> UUID
 *          - "frequency" -> UUID
 *
 */
class FieldManager(store: Store, val keyspace: Keyspace, zk: ZookeeperClient, fieldBus: FieldBus) extends Actor with ActorLogging with CassandraCFOperations {
  import FieldManager._

  import context.dispatcher

  val shardingFactor = 3
  var currentFields = FieldsChanged(Map.empty, Map.empty)
  var creatingFields: Set[FieldIdentifier] = Set.empty
  var changingFields: Set[FieldIdentifier] = Set.empty
  var removingFields: Set[FieldIdentifier] = Set.empty

  fieldBus.subscribe(self, classOf[FieldOperation])
  getFields pipeTo self

  log.debug("started {}", self.path.name)

  def receive = {

    /* notify all subscribers that fields have changed */
    case fieldsChanged: FieldsChanged =>
      fieldBus.publish(fieldsChanged)

    /* send current fields to sender */
    case GetFields(sender) =>
      sender ! currentFields

    /* create a new field */
    case CreateField(field) =>

    /* a new field was created locally */
    case CreatedField(field) =>
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
      val FieldsChanged(fieldsById, columnFamiliesById) = fieldsChanged
      val fieldPath = basepath + "/" + fieldNode
      val id = zk.client.getData.forPath(fieldPath).mkString
      val fieldNodeParts = fieldNode.split(":", 2)
      val fieldType = EventValueType.withName(fieldNodeParts(0))
      val fieldName = fieldNodeParts(1)
      val created = new DateTime(zk.client.getData.forPath(fieldPath + "/created").mkString.toLong, DateTimeZone.UTC)
      val fieldId = FieldIdentifier(fieldName, fieldType)
      //
      if (fieldsById.contains(fieldId))
        throw new Exception("field %s:%s already exists".format(fieldType.toString, fieldName))
      //
      val _fieldsChanged = fieldType match {
        case EventValueType.TEXT =>
          val fcf = new TypedFieldColumnFamily(fieldName, id, shardingFactor, new TextField(),
            new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Text))
          val field = Field(fieldName, created, text = Some(fcf))
          FieldsChanged(fieldsById ++ Map(fieldId -> field), columnFamiliesById ++ Map(fcf.id -> fcf))
      }
      log.debug("loaded field {}:{} with id {}", fieldType.toString, fieldName, id)
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
    val id = new UUIDLike(UUID.randomUUID())
    val created = DateTime.now(DateTimeZone.UTC)
    zk.client.inTransaction()
      .create().forPath(path)
      .and()
      .create().forPath(path + "/id", id.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
      .and()
      .create().forPath(path + "/created", created.getMillis.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
      .and()
      .commit()
    fieldId.fieldType match {
      case EventValueType.TEXT =>
        val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new TextField(),
          new ColumnFamily[java.lang.Long,StringPosting](id.toString, LongSerializer.get, FieldSerializers.Text))
        val field = Field(fieldId.fieldName, created, text = Some(fcf))
        val schemaChangeResult = createTextField(field).getResult
        log.debug("created field {}:{} in store {} (schema change id is {}",
          fieldId.fieldName, fieldId.fieldType.toString, store.name, schemaChangeResult.getSchemaId)
        CreatedField(field)
    }
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
    name: String, created: DateTime,
    text: Option[TypedFieldColumnFamily[TextField,StringPosting]] = None,
    literal: Option[TypedFieldColumnFamily[LiteralField,StringPosting]] = None,
    integer: Option[TypedFieldColumnFamily[IntegerField,LongPosting]] = None,
    float: Option[TypedFieldColumnFamily[FloatField,DoublePosting]] = None,
    datetime: Option[TypedFieldColumnFamily[DatetimeField,DatePosting]] = None,
    address: Option[TypedFieldColumnFamily[AddressField,AddressPosting]] = None,
    hostname: Option[TypedFieldColumnFamily[HostnameField,StringPosting]] = None)

  case class CreatedField(field: Field)

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

  sealed trait FieldOperation extends FieldEvent
  case class GetFields(sender: ActorRef) extends FieldOperation
  case class CreateField(field: FieldIdentifier) extends FieldOperation

  sealed trait FieldNotification extends FieldEvent
  case class FieldsChanged(fieldsById: Map[FieldIdentifier,Field], columnFamiliesById: Map[String,FieldColumnFamily]) extends FieldNotification
}
