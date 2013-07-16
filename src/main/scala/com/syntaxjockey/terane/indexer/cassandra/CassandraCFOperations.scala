package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.ddl.SchemaChangeResult
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.EventValueType

trait CassandraCFOperations {

  implicit val keyspace: Keyspace

  def createTextField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }

  def createLiteralField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }

  def createIntegerField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(LongType,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }

  def createFloatField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(DoubleType,TimeUUIDType)")   // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }

  def createDatetimeField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(DateType,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }

  def createAddressField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(BytesType,TimeUUIDType)")    // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }

  def createHostnameField(fieldName: String): OperationResult[SchemaChangeResult] = {
    val opts = new java.util.HashMap[String,Object]()
    opts.put("name", fieldName)
    opts.put("key_validation_class", "BytesType")                           // Row Key
    opts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    opts.put("default_validation_class", "BytesType")                       // Column Value
    keyspace.createColumnFamily(opts)
  }
}
