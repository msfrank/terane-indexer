package com.syntaxjockey.terane.indexer

import akka.actor.{Props, Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.sink.CassandraSink
import com.syntaxjockey.terane.indexer.bier.{Query, Event}
import java.util.{Date, UUID}
import org.joda.time.DateTime

class EventRouter extends Actor with ActorLogging {
  import EventRouter._

  val csSink = context.actorOf(Props(new CassandraSink("store")), "cassandra-sink")
  val queries = scala.collection.mutable.LinkedHashMap[UUID,Query]()

  def receive = {

    /* store event in the appropriate sink */
    case event: Event =>
      log.debug("forwarding event from {} to {}", sender.path.name, csSink.path.name)
      csSink forward event

    /* create a new query */
    case createQuery @ CreateQuery(query, stores, fields, limit, reverse) =>
      val query = new Query(createQuery)
      queries(query.id) = query
      log.debug("created query " + query.id)
      sender ! CreateQueryResponse(query.id)

    /* describe a query */
    case DescribeQuery(id) =>
      queries.get(id) match {
        case Some(query) =>
          sender ! DescribeQueryResponse(query.id, query.created)
        case None =>
          sender ! new Exception("no such query " + id)
      }

    /* retrieve next batch of events matching the query */
    case GetEvents(id) =>
      queries.get(id) match {
        case Some(query) =>
          log.debug("retrieving batch of events for " + id)
          csSink forward query.query
        case None =>
          sender ! new Exception("no such query " + id)
      }

    /* delete the query */
    case DeleteQuery(id) =>
      queries.get(id) match {
        case Some(query) =>
          queries.remove(id)
          log.debug("deleted query " + id)
        case None =>
          sender ! new Exception("no such query " + id)
      }
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