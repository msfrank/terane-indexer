package com.syntaxjockey.terane.indexer.sink

import com.netflix.astyanax.ddl.SchemaChangeResult
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult

trait CassandraCFOperations {

  implicit val keyspace: Keyspace

  def createTextField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.text.get.cf, opts)
  }

  def createLiteralField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.literal.get.cf, opts)
  }

  def createIntegerField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(LongType,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.integer.get.cf, opts)
  }

  def createFloatField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(DoubleType,TimeUUIDType)")   // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.float.get.cf, opts)
  }

  def createDatetimeField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(DateType,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.datetime.get.cf, opts)
  }

  def createAddressField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(BytesType,TimeUUIDType)")    // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.address.get.cf, opts)
  }

  def createHostnameField(field: Field): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(field.hostname.get.cf, opts)
  }
}
