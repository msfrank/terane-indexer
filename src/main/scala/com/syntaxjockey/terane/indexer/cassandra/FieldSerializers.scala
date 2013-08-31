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

import java.util.{Date, UUID}
import com.netflix.astyanax.annotations.{Component => AstyanaxComponent}
import com.netflix.astyanax.serializers.{ByteBufferOutputStream, CompositeRangeBuilder, AnnotatedCompositeSerializer}
import scala.annotation.meta.field
import com.netflix.astyanax.util.TimeUUIDUtils
import com.netflix.astyanax.model.Equality
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer
import java.nio.ByteBuffer

abstract class Posting

// see http://blog.fakod.eu/2010/07/14/constructor-arguments-with-jpa-annotations/
object Posting {
  type Component = AstyanaxComponent @field
}

import Posting.Component

class StringPosting(@Component(ordinal = 0) var term: String, @Component(ordinal = 1) var id: UUID) extends Posting {
  def this() = this("", FieldSerializers.emptyUUID)
}

class LongPosting(@Component(ordinal = 0) var term: Long, @Component(ordinal = 1) var id: UUID) extends Posting {
  def this() = this(0L, FieldSerializers.emptyUUID)
}

class DoublePosting(@Component(ordinal = 0) var term: Double, @Component(ordinal = 1) var id: UUID) extends Posting {
  def this() = this(0.0, FieldSerializers.emptyUUID)
}

class DatePosting(@Component(ordinal = 0) var term: Date, @Component(ordinal = 1) var id: UUID) extends Posting {
  def this() = this(FieldSerializers.emptyDate, FieldSerializers.emptyUUID)
}

class AddressPosting(@Component(ordinal = 0) var term: Array[Byte], @Component(ordinal = 1) var id: UUID) extends Posting {
  def this() = this(FieldSerializers.emptyAddress, FieldSerializers.emptyUUID)
}

object FieldSerializers {
  val emptyUUID = new UUID(0, 0)
  val emptyDate = new Date(0)
  val emptyAddress = Array[Byte](0x00, 0x00, 0x00, 0x00)

  val Text = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val Literal = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val Integer = new AnnotatedCompositeSerializer[LongPosting](classOf[LongPosting])
  val Float = new AnnotatedCompositeSerializer[DoublePosting](classOf[DoublePosting])
  val Datetime = new AnnotatedCompositeSerializer[DatePosting](classOf[DatePosting])
  val Address = new AnnotatedCompositeSerializer[AddressPosting](classOf[AddressPosting])
  val Hostname = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
}
