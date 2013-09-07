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
import akka.actor.Status.Failure
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.ddl.SchemaChangeResult
import com.netflix.curator.framework.recipes.locks.InterProcessReadWriteLock
import org.apache.zookeeper.data.Stat
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future
import scala.collection.JavaConversions._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.cassandra._
import com.syntaxjockey.terane.indexer.zookeeper._
import com.syntaxjockey.terane.indexer.metadata.Store
import com.syntaxjockey.terane.indexer.UUIDLike

/**
 *
 *  + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      + "fields"
 *        + fieldTypeAndName: String -> id: UUIDLike
 *          - "created" -> Long
 *
 */
class FieldManager(store: Store, val keyspace: Keyspace, sinkBus: SinkBus) extends Actor with ActorLogging with CassandraCFOperations {
  import FieldManager._
  import UUIDLike._

  import context.dispatcher

  val zk = Zookeeper(context.system).client

  val shardingFactor = 3
  var currentFields = FieldMap(Map.empty, Map.empty)
  var creatingFields: Set[FieldIdentifier] = Set.empty
  var changingFields: Set[FieldIdentifier] = Set.empty
  var removingFields: Set[FieldIdentifier] = Set.empty

  getFields pipeTo self

  log.debug("started {}", self.path.name)

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
    case CreateField(fieldId) if !creatingFields.contains(fieldId) =>
      creatingFields = creatingFields + fieldId
      log.debug("creating field {}", fieldId)
      createField(fieldId) pipeTo self

    /* a new field was created */
    case CreatedField(fieldId, field, fcf, schemaChangeId) =>
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
   *
   * @return
   */
  def getFields = Future[FieldMap] {
    val basepath = "/stores/" + store.name + "/fields"
    val znodes = zk.getChildren.forPath(basepath)
    log.debug("found {} fields in {}", znodes.length, basepath)
    znodes.foldLeft(FieldMap(Map.empty, Map.empty)) {
      (fieldsChanged, fieldNode) =>
      val FieldMap(fieldsByIdent, fieldsByCf) = fieldsChanged
      val fieldPath = basepath + "/" + fieldNode
      val id = new String(zk.getData.forPath(fieldPath), Zookeeper.UTF_8_CHARSET)
      val fieldNodeParts = fieldNode.split(":", 2)
      val fieldType = DataType.withName(fieldNodeParts(0))
      val fieldName = fieldNodeParts(1)
      val createdString = new String(zk.getData.forPath(fieldPath + "/created"), Zookeeper.UTF_8_CHARSET)
      val created = new DateTime(createdString.toLong, DateTimeZone.UTC)
      val fieldId = FieldIdentifier(fieldName, fieldType)
      if (fieldsByIdent.contains(fieldId))
        throw new Exception("field %s:%s already exists".format(fieldType.toString, fieldName))
      /* create the new field column family and update the field maps */
      val _fieldsChanged = fieldType match {
        case DataType.TEXT =>
          val fcf = new TypedFieldColumnFamily(fieldName, id, shardingFactor, new TextField(),
            new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Text))
          val field = CassandraField(fieldId, created, text = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.LITERAL =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new LiteralField(),
            new ColumnFamily[java.lang.Long,StringPosting](id.toString, LongSerializer.get, FieldSerializers.Literal))
          val field = CassandraField(fieldId, created, literal = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.INTEGER =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new IntegerField(),
            new ColumnFamily[java.lang.Long,LongPosting](id.toString, LongSerializer.get, FieldSerializers.Integer))
          val field = CassandraField(fieldId, created, integer = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.FLOAT =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new FloatField(),
            new ColumnFamily[java.lang.Long,DoublePosting](id.toString, LongSerializer.get, FieldSerializers.Float))
          val field = CassandraField(fieldId, created, float = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.DATETIME =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new DatetimeField(),
            new ColumnFamily[java.lang.Long,DatePosting](id.toString, LongSerializer.get, FieldSerializers.Datetime))
          val field = CassandraField(fieldId, created, datetime = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.ADDRESS =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new AddressField(),
            new ColumnFamily[java.lang.Long,AddressPosting](id.toString, LongSerializer.get, FieldSerializers.Address))
          val field = CassandraField(fieldId, created, address = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
        case DataType.HOSTNAME =>
          val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id.toString, shardingFactor, new HostnameField(),
            new ColumnFamily[java.lang.Long,StringPosting](id.toString, LongSerializer.get, FieldSerializers.Hostname))
          val field = CassandraField(fieldId, created, hostname = Some(fcf))
          FieldMap(fieldsByIdent ++ Map(fieldId -> field), fieldsByCf ++ Map(fcf.id -> field))
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
    val lock = new InterProcessReadWriteLock(zk, "/lock" + path)
    val writeLock = lock.writeLock()
    writeLock.acquire()
    try {
      /* check whether field already exists */
      zk.checkExists().forPath(path) match {
        case stat: Stat =>
          // FIXME: return field if it already exists
          throw new Exception("field already exists")
        case null =>
          /* create the column family in cassandra */
          val createdField = fieldId.fieldType match {
            case DataType.TEXT =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new TextField(),
                new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Text))
              val field = CassandraField(fieldId, created, text = Some(fcf))
              CreatedField(fieldId, field, fcf, createTextField(id).getResult)
            case DataType.LITERAL =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new LiteralField(),
                new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Literal))
              val field = CassandraField(fieldId, created, literal = Some(fcf))
              CreatedField(fieldId, field, fcf, createLiteralField(id).getResult)
            case DataType.INTEGER =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new IntegerField(),
                new ColumnFamily[java.lang.Long,LongPosting](id, LongSerializer.get, FieldSerializers.Integer))
              val field = CassandraField(fieldId, created, integer = Some(fcf))
              CreatedField(fieldId, field, fcf, createIntegerField(id).getResult)
            case DataType.FLOAT =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new FloatField(),
                new ColumnFamily[java.lang.Long,DoublePosting](id, LongSerializer.get, FieldSerializers.Float))
              val field = CassandraField(fieldId, created, float = Some(fcf))
              CreatedField(fieldId, field, fcf, createFloatField(id).getResult)
            case DataType.DATETIME =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new DatetimeField(),
                new ColumnFamily[java.lang.Long,DatePosting](id, LongSerializer.get, FieldSerializers.Datetime))
              val field = CassandraField(fieldId, created, datetime = Some(fcf))
              CreatedField(fieldId, field, fcf, createDatetimeField(id).getResult)
            case DataType.ADDRESS =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new AddressField(),
                new ColumnFamily[java.lang.Long,AddressPosting](id, LongSerializer.get, FieldSerializers.Address))
              val field = CassandraField(fieldId, created, address = Some(fcf))
              CreatedField(fieldId, field, fcf, createAddressField(id).getResult)
            case DataType.HOSTNAME =>
              val fcf = new TypedFieldColumnFamily(fieldId.fieldName, id, shardingFactor, new HostnameField(),
                new ColumnFamily[java.lang.Long,StringPosting](id, LongSerializer.get, FieldSerializers.Hostname))
              val field = CassandraField(fieldId, created, hostname = Some(fcf))
              CreatedField(fieldId, field, fcf, createHostnameField(id).getResult)
          }
          /* create the field in zookeeper */
          zk.inTransaction()
            .create().forPath(path, id.toString.getBytes(Zookeeper.UTF_8_CHARSET))
            .and()
            .create().forPath(path + "/created", created.getMillis.toString.getBytes(Zookeeper.UTF_8_CHARSET))
            .and()
            .commit()
          log.debug("created field {}:{} in store {} (schema change id is {})",
            fieldId.fieldName, fieldId.fieldType.toString, store.name, createdField.schemaChangeId.getSchemaId)
          createdField
      }
    } finally {
      /* unlock field */
      writeLock.release()
    }
  }
}

object FieldManager {

  def props(store: Store, keyspace: Keyspace, sinkBus: SinkBus) = {
    Props(classOf[FieldManager], store, keyspace, sinkBus)
  }

  case object GetFields
  case class CreateField(fieldId: FieldIdentifier)
  case class CreatedField(fieldId: FieldIdentifier, field: CassandraField, fcf: FieldColumnFamily, schemaChangeId: SchemaChangeResult)
  case class DeleteField(field: FieldIdentifier)
  case class DeletedField(fieldId: FieldIdentifier, field: CassandraField, schemaChangeId: SchemaChangeResult)

  sealed trait FieldEvent extends SinkEvent
  sealed trait FieldNotification extends FieldEvent
  case class FieldMap(fieldsByIdent: Map[FieldIdentifier,CassandraField], fieldsByCf: Map[String,CassandraField]) extends FieldNotification
}

case class CassandraField(fieldId: FieldIdentifier,
                          created: DateTime,
                          text: Option[TypedFieldColumnFamily[TextField,StringPosting]] = None,
                          literal: Option[TypedFieldColumnFamily[LiteralField,StringPosting]] = None,
                          integer: Option[TypedFieldColumnFamily[IntegerField,LongPosting]] = None,
                          float: Option[TypedFieldColumnFamily[FloatField,DoublePosting]] = None,
                          datetime: Option[TypedFieldColumnFamily[DatetimeField,DatePosting]] = None,
                          address: Option[TypedFieldColumnFamily[AddressField,AddressPosting]] = None,
                          hostname: Option[TypedFieldColumnFamily[HostnameField,StringPosting]] = None)
