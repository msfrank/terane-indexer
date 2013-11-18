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
  val postings: Option[ColumnFamily[UUID,T]]
  val kgrams: Option[ColumnFamily[T,T]]
}

final case class TextFieldCF(
  name: String,
  id: String,
  width: Long,
  field: TextField,
  terms: ColumnFamily[java.lang.Long,StringPosting],
  postings: Option[ColumnFamily[UUID,String]],
  kgrams: Option[ColumnFamily[String,String]])
extends TypedFieldColumnFamily[TextField,StringPosting,String]

final case class LiteralFieldCF(
  name: String,
  id: String,
  width: Long,
  field: LiteralField,
  terms: ColumnFamily[java.lang.Long,StringPosting],
  postings: Option[ColumnFamily[UUID,String]],
  kgrams: Option[ColumnFamily[String,String]])
extends TypedFieldColumnFamily[LiteralField,StringPosting,String]

final case class IntegerFieldCF(
  name: String,
  id: String,
  width: Long,
  field: IntegerField,
  terms: ColumnFamily[java.lang.Long,LongPosting],
  postings: Option[ColumnFamily[UUID,Long]])
extends TypedFieldColumnFamily[IntegerField,LongPosting,Long] { val kgrams = None }

final case class FloatFieldCF(
  name: String,
  id: String,
  width: Long,
  field: FloatField,
  terms: ColumnFamily[java.lang.Long,DoublePosting],
  postings: Option[ColumnFamily[UUID,Double]])
extends TypedFieldColumnFamily[FloatField,DoublePosting,Double] { val kgrams = None }

final case class DatetimeFieldCF(
  name: String,
  id: String,
  width: Long,
  field: DatetimeField,
  terms: ColumnFamily[java.lang.Long,DatePosting],
  postings: Option[ColumnFamily[UUID,Date]])
extends TypedFieldColumnFamily[DatetimeField,DatePosting,Date] { val kgrams = None }

final case class AddressFieldCF(
  name: String,
  id: String,
  width: Long,
  field: AddressField,
  terms: ColumnFamily[java.lang.Long,AddressPosting],
  postings: Option[ColumnFamily[UUID,Array[Byte]]])
extends TypedFieldColumnFamily[AddressField,AddressPosting,Array[Byte]] { val kgrams = None }

final case class HostnameFieldCF(
  name: String,
  id: String,
  width: Long,
  field: HostnameField,
  terms: ColumnFamily[java.lang.Long,StringPosting],
  postings: Option[ColumnFamily[UUID,String]])
extends TypedFieldColumnFamily[HostnameField,StringPosting,String] { val kgrams = None }
