package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.EventRouter.{DescribeQueryResponse, GetEvents, DescribeQuery, CreateQuery}
import java.util.UUID
import org.joda.time.{DateTimeZone, DateTime}
import akka.actor.{Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.bier.Matchers.Posting

class Query(val id: UUID, val createContext: CreateQuery) extends Actor with ActorLogging {
  import Query._

  val created = DateTime.now(DateTimeZone.UTC)

  override def preStart() {

  }

  def receive = {
    case DescribeQuery =>
      sender ! DescribeQueryResponse(id, created)
    case GetEvents =>
  }

  /**
   * Return at most the specified number of posting ids.  This method may return
   * less if the matcher has less than maxPostings.
   *
   * @param maxPostings
   * @return
   *
  def getPostings(maxPostings: Int): List[UUID] = {
    var postings = List.empty[UUID]
    var n = 0
    while (n < maxPostings) {
      for (postingOrDone <- getNextPosting) {
        postingOrDone match {
          case Left(_) =>
            return postings
          case Right(Posting(id, _)) =>
            postings = postings :+ id
        }
      }
      n += 1
    }
    postings
  }
   */
}

object Query {
  case class BatchRequest(query: Matchers, maxResults: Int)
  case class BatchResponse()
}
