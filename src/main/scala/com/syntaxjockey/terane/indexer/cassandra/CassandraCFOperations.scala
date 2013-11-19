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
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createTextField(fcf: TextFieldColumnFamily): Try[TextFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                          // Row Key
    termsOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")    // Column Key
    termsOpts.put("default_validation_class", "BytesType")                      // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")                        // Row Key
    postingsOpts.put("comparator_type", "UTF8Type")                             // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case failure @ Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createTextField(name: String, id: String, width: Long): Try[TextFieldColumnFamily] = createTextField(new TextFieldColumnFamily(name, id, width))

  /**
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createLiteralField(fcf: LiteralFieldColumnFamily): Try[LiteralFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
    postingsOpts.put("comparator_type", "UTF8Type")         // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createLiteralField(name: String, id: String, width: Long): Try[LiteralFieldColumnFamily] = createLiteralField(new LiteralFieldColumnFamily(name, id, width))

  /**
   *
   */
  /**
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createIntegerField(fcf: IntegerFieldColumnFamily): Try[IntegerFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(LongType,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
    postingsOpts.put("comparator_type", "LongType")         // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createIntegerField(name: String, id: String, width: Long): Try[IntegerFieldColumnFamily] = createIntegerField(new IntegerFieldColumnFamily(name, id, width))

  /**
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createFloatField(fcf: FloatFieldColumnFamily): Try[FloatFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(DoubleType,TimeUUIDType)")   // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
    postingsOpts.put("comparator_type", "DoubleType")       // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createFloatField(name: String, id: String, width: Long): Try[FloatFieldColumnFamily] = createFloatField(new FloatFieldColumnFamily(name, id, width))

  /**
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createDatetimeField(fcf: DatetimeFieldColumnFamily): Try[DatetimeFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(DateType,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
    postingsOpts.put("comparator_type", "DateType")         // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createDatetimeField(name: String, id: String, width: Long): Try[DatetimeFieldColumnFamily] = createDatetimeField(new DatetimeFieldColumnFamily(name, id, width))

  /**
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createAddressField(fcf: AddressFieldColumnFamily): Try[AddressFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(BytesType,TimeUUIDType)")    // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
    postingsOpts.put("comparator_type", "BytesType")        // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createAddressField(name: String, id: String, width: Long): Try[AddressFieldColumnFamily] = createAddressField(new AddressFieldColumnFamily(name, id, width))

  /**
   * Create the column families for the specified field.  Returns the wrapped fcf
   * on success, otherwise returns Failure[Throwable].
   */
  def createHostnameField(fcf: HostnameFieldColumnFamily): Try[HostnameFieldColumnFamily] = {
    val termsOpts = new java.util.HashMap[String,Object]()
    termsOpts.put("name", fcf.terms.getName)
    termsOpts.put("key_validation_class", "BytesType")                           // Row Key
    termsOpts.put("comparator_type", "CompositeType(UTF8Type,TimeUUIDType)")     // Column Key
    termsOpts.put("default_validation_class", "BytesType")                       // Column Value
    val postingsOpts = new java.util.HashMap[String,Object]()
    postingsOpts.put("name", fcf.postings.getName)
    postingsOpts.put("key_validation_class", "UUIDType")    // Row Key
    postingsOpts.put("comparator_type", "UTF8Type")         // Column Key
    createColumnFamilies(Seq(termsOpts, postingsOpts)) match {
      case Failure(ex) =>
        Failure(ex)
      case _ => Success(fcf)
    }
  }

  def createHostnameField(name: String, id: String, width: Long): Try[HostnameFieldColumnFamily] = createHostnameField(new HostnameFieldColumnFamily(name, id, width))
}
