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

import com.netflix.astyanax.serializers._
import com.netflix.astyanax.model.ColumnFamily
import java.util.{Date, UUID}

import com.syntaxjockey.terane.indexer.bier._

sealed trait FieldColumnFamily {
  val name: String
  val id: String
  val width: Long
}

sealed trait TypedFieldColumnFamily[F,P,T] extends FieldColumnFamily {
  val field: F
  val terms: ColumnFamily[java.lang.Long,P]
  val postings: ColumnFamily[UUID,T]
}

/**
 *
 */
class TextFieldColumnFamily(val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[TextField,StringPosting,String] {
  val field = new TextField()
  val terms = new ColumnFamily[java.lang.Long,StringPosting](id + "_t", LongSerializer.get, Serializers.Text)
  val postings = new ColumnFamily[UUID,String](id + "_p", TimeUUIDSerializer.get, StringSerializer.get)
}

/**
 *
 */
class LiteralFieldColumnFamily(val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[LiteralField,StringPosting,String] {
  val field = new LiteralField()
  val terms = new ColumnFamily[java.lang.Long,StringPosting](id + "_t", LongSerializer.get, Serializers.Literal)
  val postings = new ColumnFamily[UUID,String](id + "_p", TimeUUIDSerializer.get, StringSerializer.get)
}

/**
 *
 */
class IntegerFieldColumnFamily(val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[IntegerField,LongPosting,java.lang.Long] {
  val field = new IntegerField()
  val terms = new ColumnFamily[java.lang.Long,LongPosting](id + "_t", LongSerializer.get, Serializers.Integer)
  val postings = new ColumnFamily[UUID,java.lang.Long](id + "_p", TimeUUIDSerializer.get, LongSerializer.get)
}

/**
 *
 */
class FloatFieldColumnFamily(val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[FloatField,DoublePosting,java.lang.Double] {
  val field = new FloatField()
  val terms =  new ColumnFamily[java.lang.Long,DoublePosting](id + "_t", LongSerializer.get, Serializers.Float)
  val postings = new ColumnFamily[UUID,java.lang.Double](id + "_p", TimeUUIDSerializer.get, DoubleSerializer.get)
}

/**
 *
 */
class DatetimeFieldColumnFamily( val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[DatetimeField,DatePosting,Date] {
  val field = new DatetimeField()
  val terms = new ColumnFamily[java.lang.Long,DatePosting](id + "_t", LongSerializer.get, Serializers.Datetime)
  val postings = new ColumnFamily[UUID,Date](id + "_p", TimeUUIDSerializer.get, DateSerializer.get)
}

/**
 *
 */
class AddressFieldColumnFamily( val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[AddressField,AddressPosting,Array[Byte]] {
  val field = new AddressField()
  val terms = new ColumnFamily[java.lang.Long,AddressPosting](id + "_t", LongSerializer.get, Serializers.Address)
  val postings = new ColumnFamily[UUID,Array[Byte]](id + "_p", TimeUUIDSerializer.get, BytesArraySerializer.get)
}

/**
 *
 */
class HostnameFieldColumnFamily( val name: String, val id: String, val width: Long) extends TypedFieldColumnFamily[HostnameField,StringPosting,String] {
  val field = new HostnameField()
  val terms = new ColumnFamily[java.lang.Long,StringPosting](id + "_t", LongSerializer.get, Serializers.Hostname)
  val postings = new ColumnFamily[UUID,String](id + "_p", TimeUUIDSerializer.get, StringSerializer.get)
}
