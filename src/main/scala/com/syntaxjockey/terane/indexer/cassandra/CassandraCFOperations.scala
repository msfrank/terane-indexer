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
import scala.util.{Success, Failure, Try}

trait CassandraCFOperations {

  implicit val keyspace: Keyspace

  private def createColumnFamilies(cfOpts: Seq[java.util.HashMap[String,Object]]): Try[Unit] = {
    try {
      Success(cfOpts.foreach(keyspace.createColumnFamily))
    } catch {
      case ex: Throwable => Failure(ex)
    }
  }

  /**
   * Create the column families for the specified field.  Returns Success(Unit)
   * on success, otherwise returns Failure[Throwable].
   */
  def createTextField(fcf: TextFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "UTF8Type")         // Column Key
      opts = opts :+ postingsOpts
    }
    fcf.kgrams.foreach { cf =>
      val kgramsOpts = new java.util.HashMap[String,Object]()
      kgramsOpts.put("name", fcf.id + "_k")
      kgramsOpts.put("key_validation_class", "UTF8Type")    // Row Key
      kgramsOpts.put("comparator_type", "UTF8Type")         // Column Key
      opts = opts :+ kgramsOpts
    }
    createColumnFamilies(opts)
  }

  def createLiteralField(fcf: LiteralFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "UTF8Type")         // Column Key
      opts = opts :+ postingsOpts
    }
    fcf.kgrams.foreach { cf =>
      val kgramsOpts = new java.util.HashMap[String,Object]()
      kgramsOpts.put("name", fcf.id + "_k")
      kgramsOpts.put("key_validation_class", "UTF8Type")    // Row Key
      kgramsOpts.put("comparator_type", "UTF8Type")         // Column Key
      opts = opts :+ kgramsOpts
    }
    createColumnFamilies(opts)
  }

  def createIntegerField(fcf: IntegerFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(LongType,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "LongType")         // Column Key
      opts = opts :+ postingsOpts
    }
    createColumnFamilies(opts)
  }

  def createFloatField(fcf: FloatFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(DoubleType,TimeUUIDType)")   // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "DoubleType")       // Column Key
      opts = opts :+ postingsOpts
    }
    createColumnFamilies(opts)
  }

  def createDatetimeField(fcf: DatetimeFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(DateType,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "DateType")         // Column Key
      opts = opts :+ postingsOpts
    }
    createColumnFamilies(opts)
  }

  def createAddressField(fcf: AddressFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(BytesType,TimeUUIDType)")    // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "BytesType")        // Column Key
      opts = opts :+ postingsOpts
    }
    createColumnFamilies(opts)
  }

  def createHostnameField(fcf: HostnameFieldCF): Try[Unit] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.id)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    var opts = Seq(termsOpts)
    fcf.postings.foreach { cf =>
      val postingsOpts = new java.util.HashMap[String,Object]()
      postingsOpts.put("name", fcf.id + "_p")
      postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
      postingsOpts.put("comparator_type", "UTF8Type")         // Column Key
      opts = opts :+ postingsOpts
    }
    createColumnFamilies(opts)
  }
}
