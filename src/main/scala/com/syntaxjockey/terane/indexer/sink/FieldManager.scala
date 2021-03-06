/**
 * Copyright 2013 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Terane.
 *
 * Terane is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Terane is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Props, Actor, ActorLogging}
import akka.pattern.pipe
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{StringSerializer, LongSerializer}
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.ddl.SchemaChangeResult
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock
import org.apache.zookeeper.data.Stat
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.util.Failure
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.cassandra._
import com.syntaxjockey.terane.indexer.zookeeper._
import com.syntaxjockey.terane.indexer.UUIDLike

/**
 * FieldManager is responsible for creating and deleting fields
 */
class FieldManager(settings: CassandraSinkSettings, zookeeperPath: String, val keyspace: Keyspace, sinkBus: SinkBus) extends Actor with ActorLogging with CassandraCFOperations {
  import FieldManager._
  import UUIDLike._

  import context.dispatcher

  // config
  val shardingFactor = 3
  val zookeeper = Zookeeper(context.system).client

  // state
  var currentFields = FieldMap(Map.empty, Map.empty)
  var creatingFields: Set[FieldIdentifier] = Set.empty
  var changingFields: Set[FieldIdentifier] = Set.empty
  var removingFields: Set[FieldIdentifier] = Set.empty

  override def preStart() {
    getFields pipeTo self
  }

  def receive = {

    /* notify all subscribers that fields have changed */
    case fieldsChanged: FieldMap =>
      log.debug("fields have changed")
      currentFields = fieldsChanged
      sinkBus.publish(currentFields)

    /* send current fields to sender */
    case GetFields =>
      sender ! currentFields

    /* create a new field */
    case op @ CreateField(fieldId) if !creatingFields.contains(fieldId) =>
      creatingFields = creatingFields + fieldId
      log.debug("creating field {}", fieldId)
      createField(op) pipeTo self

    /* a new field was created */
    case CreatedField(fieldId, field, fcf) =>
      val fieldsByIdent = currentFields.fieldsByIdent
      val fieldsByCf = currentFields.fieldsByCf
      currentFields = FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
      creatingFields = creatingFields - fieldId
      sinkBus.publish(currentFields)

    case Failure(cause) =>
      log.debug("received failure: {}", cause.getMessage)
  }

  /**
   * Asynchronously retrieve the list of fields.
   */
  def getFields = Future[FieldMap] {
    val basepath = zookeeperPath + "/fields"
    val znodes = zookeeper.getChildren.forPath(basepath)
    log.debug("found {} fields in {}", znodes.length, basepath)
    znodes.foldLeft(FieldMap(Map.empty, Map.empty)) {
      (fieldsChanged, fieldNode) =>
      val FieldMap(fieldsByIdent, fieldsByCf) = fieldsChanged
      val fieldPath = basepath + "/" + fieldNode
      val id = new String(zookeeper.getData.forPath(fieldPath), Zookeeper.UTF_8_CHARSET)
      val fieldNodeParts = fieldNode.split(":", 2)
      val fieldType = DataType.withName(fieldNodeParts(0))
      val fieldName = fieldNodeParts(1)
      val createdString = new String(zookeeper.getData.forPath(fieldPath + "/created"), Zookeeper.UTF_8_CHARSET)
      val created = new DateTime(createdString.toLong, DateTimeZone.UTC)
      val fieldId = FieldIdentifier(fieldName, fieldType)
      if (fieldsByIdent.contains(fieldId))
        throw new Exception("field %s:%s already exists".format(fieldType.toString, fieldName))
      /* create the new field column family and update the field maps */
      val _fieldsChanged = fieldType match {
        case DataType.TEXT =>
          val fcf = new TextFieldColumnFamily(fieldName, id, shardingFactor)
          val field = CassandraField(fieldId, created, text = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.LITERAL =>
          val fcf = new LiteralFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor)
          val field = CassandraField(fieldId, created, literal = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.INTEGER =>
          val fcf = new IntegerFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor)
          val field = CassandraField(fieldId, created, integer = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.FLOAT =>
          val fcf = new FloatFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor)
          val field = CassandraField(fieldId, created, float = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.DATETIME =>
          val fcf = new DatetimeFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor)
          val field = CassandraField(fieldId, created, datetime = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.ADDRESS =>
          val fcf = new AddressFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor)
          val field = CassandraField(fieldId, created, address = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.HOSTNAME =>
          val fcf = new HostnameFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor)
          val field = CassandraField(fieldId, created, hostname = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
      }
      log.debug("found field {}:{} with id {}", fieldType.toString, fieldName, id)
      _fieldsChanged
    }
  }

  /**
   * Asynchronously create a new field.
   */
  def createField(op: CreateField) = Future[FieldModificationResult] {
    val fieldId = op.fieldId
    val path = zookeeperPath + "/fields/" + fieldId.fieldType.toString + ":" + fieldId.fieldName
    val id: UUIDLike = UUID.randomUUID()
    val created = DateTime.now(DateTimeZone.UTC)
    /* create the cassandra field column family */
    val result = fieldId.fieldType match {
      case DataType.TEXT =>
        val fcf = createTextField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, text = Some(fcf))
        CreatedField(fieldId, field, fcf)
      case DataType.LITERAL =>
        val fcf = createLiteralField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, literal = Some(fcf))
        CreatedField(fieldId, field, fcf)
      case DataType.INTEGER =>
        val fcf = createIntegerField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, integer = Some(fcf))
        CreatedField(fieldId, field, fcf)
      case DataType.FLOAT =>
        val fcf = createFloatField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, float = Some(fcf))
        CreatedField(fieldId, field, fcf)
      case DataType.DATETIME =>
        val fcf = createDatetimeField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, datetime = Some(fcf))
        CreatedField(fieldId, field, fcf)
      case DataType.ADDRESS =>
        val fcf = createAddressField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, address = Some(fcf))
        CreatedField(fieldId, field, fcf)
      case DataType.HOSTNAME =>
        val fcf = createHostnameField(fieldId.fieldName, id, shardingFactor).get
        val field = CassandraField(fieldId, created, hostname = Some(fcf))
        CreatedField(fieldId, field, fcf)
    }
    try {
      /* create the field in zookeeper */
      zookeeper.inTransaction()
        .create().forPath(path, id.toString.getBytes(Zookeeper.UTF_8_CHARSET))
        .and()
        .create().forPath(path + "/created", created.getMillis.toString.getBytes(Zookeeper.UTF_8_CHARSET))
        .and()
        .commit()
      log.debug("created field {}:{}", fieldId.fieldName, fieldId.fieldType.toString)
      result
    } catch {
      case ex: Throwable => ModificationFailed(ex, op)
    }
  }
}

object FieldManager {

  def props(settings: CassandraSinkSettings, zookeeperPath: String, keyspace: Keyspace, sinkBus: SinkBus) = {
    Props(classOf[FieldManager], settings, zookeeperPath, keyspace, sinkBus)
  }

  val CF_FIELDS = new ColumnFamily[String,MetaKey]("meta", StringSerializer.get(), Serializers.Meta)

  case object GetFields

  sealed trait FieldModification
  sealed trait FieldModificationResult
  case class CreateField(fieldId: FieldIdentifier) extends FieldModification
  case class CreatedField(fieldId: FieldIdentifier, field: CassandraField, fcf: FieldColumnFamily) extends FieldModificationResult
  case class DeleteField(field: FieldIdentifier) extends FieldModification
  case class DeletedField(fieldId: FieldIdentifier, field: CassandraField) extends FieldModificationResult
  case class ModificationFailed(cause: Throwable, op: FieldModification) extends FieldModificationResult

  sealed trait FieldEvent extends SinkEvent
  sealed trait FieldNotification extends FieldEvent
  case class FieldMap(fieldsByIdent: Map[FieldIdentifier,CassandraField], fieldsByCf: Map[String,CassandraField]) extends FieldNotification
}

case class CassandraField(
  fieldId: FieldIdentifier,
  created: DateTime,
  text: Option[TextFieldColumnFamily] = None,
  literal: Option[LiteralFieldColumnFamily] = None,
  integer: Option[IntegerFieldColumnFamily] = None,
  float: Option[FloatFieldColumnFamily] = None,
  datetime: Option[DatetimeFieldColumnFamily] = None,
  address: Option[AddressFieldColumnFamily] = None,
  hostname: Option[HostnameFieldColumnFamily] = None)
