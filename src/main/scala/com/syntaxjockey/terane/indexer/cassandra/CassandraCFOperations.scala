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

package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.ddl.SchemaChangeResult

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
