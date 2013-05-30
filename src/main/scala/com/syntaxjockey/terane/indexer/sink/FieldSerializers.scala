package com.syntaxjockey.terane.indexer.sink

import java.util.{Date, UUID}
import com.netflix.astyanax.annotations.{Component => AstyanaxComponent}
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer
import scala.annotation.meta.field

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

object FieldSerializers {
  val emptyUUID = new UUID(0, 0)
  val emptyDate = new Date(0)

  val Text = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val Literal = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val Integer = new AnnotatedCompositeSerializer[LongPosting](classOf[LongPosting])
  val Float = new AnnotatedCompositeSerializer[DoublePosting](classOf[DoublePosting])
  val Datetime = new AnnotatedCompositeSerializer[DatePosting](classOf[DatePosting])
  val Hostname = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
}

