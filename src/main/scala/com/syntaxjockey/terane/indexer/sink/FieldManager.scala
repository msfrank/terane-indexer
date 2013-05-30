package com.syntaxjockey.terane.indexer.sink

import com.netflix.astyanax.model.ColumnFamily
import akka.event.LoggingAdapter
import com.netflix.astyanax.{Cluster, Keyspace}
import com.syntaxjockey.terane.indexer.bier._
import java.util.UUID
import com.netflix.astyanax.serializers.{LongSerializer, StringSerializer}

trait FieldManager {
  import FieldManager._

  def log: LoggingAdapter
  def csKeyspace: Keyspace
  def csCluster: Cluster

  /* declare our field -> column family mappings */
  private val textColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[TextField,StringPosting]]()
  private val literalColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[LiteralField,StringPosting]]()
  private val integerColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[IntegerField,LongPosting]]()
  private val floatColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[FloatField,DoublePosting]]()
  private val datetimeColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[DatetimeField,DatePosting]]()
  private val addressColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[AddressField,StringPosting]]()
  private val hostnameColumnFamilies = scala.collection.mutable.HashMap[String,TypedFieldColumnFamily[HostnameField,StringPosting]]()

  private val fieldsById = scala.collection.mutable.HashMap[UUID,(Field,FieldColumnFamily)]()

  def getField(id: UUID): Option[(Field,FieldColumnFamily)] = {
    fieldsById.get(id)
  }

  def idToColumnName(id: UUID): String = {
    "%016x%016x".format(id.getMostSignificantBits, id.getLeastSignificantBits)
  }

  def getOrCreateTextField(name: String): TypedFieldColumnFamily[TextField,StringPosting] = {
    textColumnFamilies.get(name) match {
      case None =>
        val id = UUID.randomUUID()
        val columnName = idToColumnName(id)
        val fcf = new TypedFieldColumnFamily(name, id, 3, new TextField(),
          new ColumnFamily[java.lang.Long,StringPosting](columnName, LongSerializer.get, FieldSerializers.Text))
        val cfOpts = new java.util.HashMap[String,Object]()
        cfOpts.put("key_validation_class", "BytesType")                           // Row Key
        cfOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
        cfOpts.put("default_validation_class", "BytesType")                       // Column Value
        csKeyspace.createColumnFamily(fcf.cf, cfOpts)
        fieldsById(id) = (fcf.field, fcf)
        textColumnFamilies(name) = fcf
        log.debug("created text field {} with id {}", name, columnName)
        fcf
      case Some(fcf) =>
        fcf
    }
  }

  def getOrCreateLiteralField(name: String): TypedFieldColumnFamily[LiteralField,StringPosting] = {
    literalColumnFamilies.get(name) match {
      case None =>
        val id = UUID.randomUUID()
        val columnName = idToColumnName(id)
        val fcf = new TypedFieldColumnFamily(name, id, 3, new LiteralField(),
          new ColumnFamily[java.lang.Long,StringPosting](columnName, LongSerializer.get, FieldSerializers.Literal))
        val cfOpts = new java.util.HashMap[String,Object]()
        cfOpts.put("key_validation_class", "BytesType")                           // Row Key
        cfOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
        cfOpts.put("default_validation_class", "BytesType")                       // Column Value
        csKeyspace.createColumnFamily(fcf.cf, cfOpts)
        fieldsById(id) = (fcf.field,fcf)
        literalColumnFamilies(name) = fcf
        log.debug("created literal field {} with id {}", name, columnName)
        fcf
      case Some(fcf) =>
        fcf
    }
  }

  def getOrCreateIntegerField(name: String): TypedFieldColumnFamily[IntegerField,LongPosting] = {
    integerColumnFamilies.get(name) match {
      case None =>
        val id = UUID.randomUUID()
        val columnName = idToColumnName(id)
        val fcf = new TypedFieldColumnFamily(name, id, 3, new IntegerField(),
          new ColumnFamily[java.lang.Long,LongPosting](columnName, LongSerializer.get, FieldSerializers.Integer))
        val cfOpts = new java.util.HashMap[String,Object]()
        cfOpts.put("key_validation_class", "BytesType")                           // Row Key
        cfOpts.put("comparator_type", "CompositeType(LongType,TimeUUIDType)")     // Column Key
        cfOpts.put("default_validation_class", "BytesType")                       // Column Value
        csKeyspace.createColumnFamily(fcf.cf, cfOpts)
        fieldsById(id) = (fcf.field,fcf)
        integerColumnFamilies(name) = fcf
        log.debug("created integer field {} with id {}", name, columnName)
        fcf
      case Some(fcf) =>
        fcf
    }
  }

  def getOrCreateDatetimeField(name: String): TypedFieldColumnFamily[DatetimeField,DatePosting] = {
    datetimeColumnFamilies.get(name) match {
      case None =>
        val id = UUID.randomUUID()
        val columnName = idToColumnName(id)
        val fcf = new TypedFieldColumnFamily(name, id, 3, new DatetimeField(),
          new ColumnFamily[java.lang.Long,DatePosting](columnName, LongSerializer.get, FieldSerializers.Datetime))
        val cfOpts = new java.util.HashMap[String,Object]()
        cfOpts.put("key_validation_class", "BytesType")                           // Row Key
        cfOpts.put("comparator_type", "CompositeType(DateType,TimeUUIDType)")     // Column Key
        cfOpts.put("default_validation_class", "BytesType")                       // Column Value
        csKeyspace.createColumnFamily(fcf.cf, cfOpts)
        fieldsById(id) = (fcf.field,fcf)
        datetimeColumnFamilies(name) = fcf
        log.debug("created datetime field {} with id {}", name, columnName)
        fcf
      case Some(fcf) =>
        fcf
    }
  }
}

object FieldManager {
  case class FieldColumnFamily(name: String, id: UUID, width: Long)
  class TypedFieldColumnFamily[F,P](
    override val name: String,
    override val id: UUID,
    override val width: Long,
    val field: F,
    val cf: ColumnFamily[java.lang.Long,P]) extends FieldColumnFamily(name, id, width)
}
