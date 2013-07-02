package com.syntaxjockey.terane.indexer.sink

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import com.netflix.astyanax.Keyspace
import scala.concurrent.{ExecutionContext, Future}

import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.Field
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldColumnFamily

class Term[T](override val field: Field,
              override val term: T,
              val keyspace: Keyspace,
              val fcf: FieldColumnFamily) extends TermMatcher[T](field, term) {

  import Matchers._

  private[this] var postings = List[Posting]()

  /*
  def nextBatch: List[Posting] = {
    val query = term match {
      case text: String =>
        val fcf = fields.getOrCreateTextField(field.fieldName)
        val range = FieldSerializers.Text.buildRange()
          .limit(100)
          .greaterThanEquals(text)
          .lessThanEquals(text)
          .build()
        keyspace.prepareQuery(fcf.cf)
          .getKey(0)
          .withColumnRange(range)
    }
    /* */
    val result = query.execute()
    val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val postings = result.getResult.map { r =>
      val positions = r.getValue(CassandraSink.SER_POSITIONS) map { i => i.toInt }
      Posting(r.getName.id, PostingMetadata(Some(positions)))
    }.toList
    postings
  }

  override def getNextPosting: Future[Either[NoMoreMatches.type,Posting]] = {
    // FIXME: shouldn't use global context
    import ExecutionContext.Implicits.global
    if (postings.size > 0) {
      val posting = postings.head
      postings = postings.tail
      Future.successful(Right(posting))
    } else {
      Future[Either[NoMoreMatches.type,Posting]] {
        val batch = nextBatch
        if (batch.size > 0) {
          postings = batch.tail
          Right(postings.head)
        } else
          Left(NoMoreMatches)
      }
    }
  }
  */

}
