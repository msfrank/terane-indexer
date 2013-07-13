package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Props, Actor, ActorLogging, ActorContext}
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.JavaConversions._
import com.netflix.astyanax.Keyspace
import scala.concurrent.Future
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field

case class Term[T](fieldId: FieldIdentifier, term: T, keyspace: Keyspace, field: Field)(implicit val context: ActorContext) extends Matchers {
  import Matchers._
  import Term._

  val fetcher = context.actorOf(Props(new TermIterator[T](this, 0)),
    "term-%s-%s-%s".format(fieldId.fieldType.toString, fieldId.fieldName, term.toString))
  implicit val timeout = Timeout(5 seconds)

  def nextPosting: Future[Either[NoMoreMatches.type,Posting]] = fetcher.ask(NextPosting).mapTo[Either[NoMoreMatches.type,Posting]]

  def findPosting(id: UUID) = Future.successful(Left(NoMoreMatches))

  def close() {
    context.stop(fetcher)
  }
}

object Term {
  case object NextPosting
  case class FindPosting(id: UUID)
}

class TermIterator[T](term: Term[T], shard: Int) extends Actor with ActorLogging {
  import Matchers._
  import Term._

  val limit = 100
  val query = term.term match {
    case text: String =>
      val range = FieldSerializers.Text.buildRange().limit(limit).greaterThanEquals(text).lessThanEquals(text).build()
      term.keyspace.prepareQuery(term.field.text.get.cf)
        .getKey(shard)
        .withColumnRange(range)
  }
  var postings: List[Posting] = query.execute().getResult.map { r =>
    val positions = r.getValue(CassandraSink.SER_POSITIONS) map { i => i.toInt }
    Matchers.Posting(r.getName.id, PostingMetadata(Some(positions)))
  }.toList

  def receive = {

    case NextPosting if postings.isEmpty =>
      sender ! Left(NoMoreMatches)

    case NextPosting if !postings.isEmpty =>
      val posting = postings.head
      postings = postings.tail
      sender ! Right(posting)
  }
}