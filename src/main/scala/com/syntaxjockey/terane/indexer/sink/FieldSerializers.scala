package com.syntaxjockey.terane.indexer.sink

import java.util.{Date, UUID}
import com.netflix.astyanax.annotations.Component
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer

class Posting

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

  val TextFieldSerializer = new AnnotatedCompositeSerializer[StringPosting](classOf[StringPosting])
  val IntegerFieldSerializer = new AnnotatedCompositeSerializer[LongPosting](classOf[LongPosting])
  val FloatFieldSerializer = new AnnotatedCompositeSerializer[DoublePosting](classOf[DoublePosting])
  val DatetimeFieldSerializer = new AnnotatedCompositeSerializer[DatePosting](classOf[DatePosting])
}

