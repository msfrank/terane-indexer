package com.syntaxjockey.terane.indexer.sink

import com.syntaxjockey.terane.indexer.bier.TermMatcher
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import java.util.UUID

import scala.collection.JavaConversions._
import com.netflix.astyanax.Keyspace

class Term[T](name: String, term: T, keyspace: Keyspace, fields: FieldManager) extends TermMatcher[T](name, term) {

  val postings: List[(UUID,PostingMetadata)] = term match {
    case text: String =>
      val fcf = fields.getOrCreateTextField(name)
      val range = FieldSerializers.Text.buildRange()
        .limit(100)
        //.withPrefix(text)
        .greaterThanEquals(text)
        .lessThanEquals(text)
        .build()
      val query = keyspace.prepareQuery(fcf.cf)
        .getKey(0)
        .withColumnRange(range)
      val result = query.execute()
      val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      val postings = result.getResult.map { r =>
        val positions = r.getValue(CassandraSink.SER_POSITIONS) map { i => i.toInt }
        (r.getName.id, PostingMetadata(Some(positions)))
      }.toList
      postings
  }

  override def iterator: Iterator[(UUID,PostingMetadata)] = {
    postings.iterator
  }

  override def size: Int = postings.size
}
