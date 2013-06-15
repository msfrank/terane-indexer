package com.syntaxjockey.terane.indexer.sink

import com.syntaxjockey.terane.indexer.bier.{Matchers, TermMatcher}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata

import scala.collection.JavaConversions._
import com.netflix.astyanax.Keyspace
import scala.concurrent.{ExecutionContext, Future}

class Term[T](override val name: String,
              override val term: T,
              val keyspace: Keyspace,
              val fields: FieldManager) extends TermMatcher[T](name, term) {

  import Matchers._

  private[this] var postings = List[Posting]()

  def nextBatch: List[Posting] = {
    val query = term match {
      case text: String =>
        val fcf = fields.getOrCreateTextField(name)
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

}
