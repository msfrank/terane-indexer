package com.syntaxjockey.terane.indexer

import akka.actor._
import com.syntaxjockey.terane.indexer.sink.CassandraSink
import com.syntaxjockey.terane.indexer.bier.{Query, Event}
import java.util.{Date, UUID}
import org.joda.time.DateTime
import scala.Some

class EventRouter extends Actor with ActorLogging {
  import EventRouter._

  val csSink = context.actorOf(Props(new CassandraSink("store")), "cassandra-sink")
  val queries = scala.collection.mutable.LinkedHashMap[UUID,ActorRef]()

  def receive = {

    /* store event in the appropriate sink */
    case event: Event =>
      log.debug("forwarding event from {} to {}", sender.path.name, csSink.path.name)
      csSink forward event

    /* create a new query */
    case createQuery: CreateQuery =>
      val id = UUID.randomUUID()
      val query = context.actorOf(Props(new Query(id, createQuery)), "query-" + id.toString)
      context.watch(query)
      queries(id) = query
      log.debug("created query " + id)
      sender ! CreateQueryResponse(id)

    /* describe a query */
    case message @ DescribeQuery(id) =>
      queries.get(id) match {
        case Some(query) =>
          log.debug("retrieving query description for " + id)
          query forward message
        case None =>
          sender ! new Exception("no such query " + id)
      }

    /* retrieve next batch of events matching the query */
    case message @ GetEvents(id) =>
      queries.get(id) match {
        case Some(query) =>
          log.debug("retrieving batch of events for " + id)
          query forward message
        case None =>
          sender ! new Exception("no such query " + id)
      }

    /* delete the query */
    case DeleteQuery(id) =>
      queries.get(id) match {
        case Some(query) =>
          query ! PoisonPill
          log.debug("deleting query " + id)
        case None =>
          sender ! new Exception("no such query " + id)
      }

    /* the query has been terminated, remove the reference */
    case Terminated(actor) =>
      val id = UUID.fromString(actor.path.name.stripPrefix("query-"))
      queries.remove(id)
      log.debug("deleted query " + id)
  }
}

object EventRouter {

  /* query case classes */
  case object ListQueries
  case class ListQueriesResponse(queries: List[DescribeQueryResponse])
  case class CreateQuery(query: String, stores: Option[List[String]], fields: Option[List[String]], limit: Option[Int], reverse: Option[Boolean])
  case class CreateQueryResponse(id: UUID)
  case class GetEvents(id: UUID)
  case class DescribeQuery(id: UUID)
  case class DescribeQueryResponse(id: UUID, created: DateTime)
  case class DeleteQuery(id: UUID)

  /* store case classes */
}