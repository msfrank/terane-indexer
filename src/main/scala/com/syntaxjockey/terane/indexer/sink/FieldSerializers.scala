package com.syntaxjockey.terane.indexer.sink

import java.util.{Date, UUID}
import com.netflix.astyanax.annotations.{Component => AstyanaxComponent}
import com.netflix.astyanax.serializers.{ByteBufferOutputStream, CompositeRangeBuilder, AnnotatedCompositeSerializer}
import scala.annotation.meta.field
import com.netflix.astyanax.util.TimeUUIDUtils
import com.netflix.astyanax.model.Equality
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer.ComponentSerializer
import java.nio.ByteBuffer

class Posting

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

  val smallestUUID = UUID.fromString("13814000-1dd2-11b2-bf91-000000000000")
  val largestUUID = UUID.fromString("138118f0-1dd2-11b2-bf91-ffffffffffff")

  val Text = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val Literal = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val Integer = new AnnotatedCompositeSerializer[LongPosting](classOf[LongPosting])
  val Float = new AnnotatedCompositeSerializer[DoublePosting](classOf[DoublePosting])
  val Datetime = new AnnotatedCompositeSerializer[DatePosting](classOf[DatePosting])
  val Address = new AnnotatedCompositeSerializer[AddressPosting](classOf[AddressPosting])
  val Hostname = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
}

/*
class FixedAnnotatedCompositeSerializer[T](clazz: Class[T]) extends AnnotatedCompositeSerializer[T](clazz) {

  override def buildRange(): CompositeRangeBuilder = {
    new FixedCompositeRangeBuilder() {
      var position = 0
      def nextComponent() {
        position += 1
      }
      def append(out: ByteBufferOutputStream, value: Object, equality: Equality) {
        val serializer: ComponentSerializer[_] = components.get(position)
        var cb: ByteBuffer = null
        try {
          cb = serializer.serializeValue(value)
        } catch {
          case ex: Exception =>
            throw new RuntimeException(ex)
        }
        if (cb == null) {
          cb = EMPTY_BYTE_BUFFER
        }
        // Write the data: <length><data><0>
        out.writeShort((short) cb.remaining());
        out.write(cb.slice());
        out.write(equality.toByte());
      }
    }
  }
}

class FixedCompositeRangeBuilder extends CompositeRangeBuilder {}
*/
