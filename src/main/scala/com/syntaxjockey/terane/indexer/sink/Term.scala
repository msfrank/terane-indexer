package com.syntaxjockey.terane.indexer.sink

import scala.collection.JavaConversions._
import com.netflix.astyanax.Keyspace
import scala.concurrent.{ExecutionContext, Future}

import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field

case class Term[T](fieldId: FieldIdentifier, term: T, keyspace: Keyspace, field: Field) extends Matchers {
  import Matchers._

  val limit = 100

  private[this] var postings = List[Posting]()

  def nextBatch(shard: Int): List[Posting] = {
    val query = term match {
      case text: String =>
        val range = FieldSerializers.Text.buildRange().limit(limit).greaterThanEquals(text).lessThanEquals(text).build()
        keyspace.prepareQuery(field.text.get.cf).getKey(shard).withColumnRange(range)
    }
    val postings = query.execute().getResult.map { r =>
      val positions = r.getValue(CassandraSink.SER_POSITIONS) map { i => i.toInt }
      Posting(r.getName.id, PostingMetadata(Some(positions)))
    }.toList
    postings
  }

  def getNextPosting: Future[Either[NoMoreMatches.type,Posting]] = {
    // FIXME: shouldn't use global context
    import ExecutionContext.Implicits.global
    if (postings.size > 0) {
      val posting = postings.head
      postings = postings.tail
      Future.successful(Right(posting))
    } else {
      Future[Either[NoMoreMatches.type,Posting]] {
        val batch = nextBatch(0)
        if (batch.size > 0) {
          postings = batch.tail
          Right(postings.head)
        } else
          Left(NoMoreMatches)
      }
    }
  }
}
