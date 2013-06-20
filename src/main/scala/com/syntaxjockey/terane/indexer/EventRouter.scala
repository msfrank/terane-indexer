package com.syntaxjockey.terane.indexer

import akka.actor._
import com.syntaxjockey.terane.indexer.sink.CassandraSink
import com.syntaxjockey.terane.indexer.bier.{Query, Event}
import java.util.UUID
import org.joda.time.DateTime
import scala.Some
import com.syntaxjockey.terane.indexer.metadata.MetadataManager
import com.syntaxjockey.terane.indexer.metadata.StoreManager

class EventRouter extends Actor with ActorLogging {
  import EventRouter._
  import MetadataManager._
  import StoreManager._

  val metadataManager = context.actorOf(Props[MetadataManager], "metadata-manager")
  val storesById = scala.collection.mutable.HashMap[UUID,Store]()
  val storesByName = scala.collection.mutable.HashMap[String,Store]()
  val sinksByName = scala.collection.mutable.HashMap[String,ActorRef]()
  val queries = scala.collection.mutable.HashMap[UUID,ActorRef]()

  def receive = {

    /* a new store was created */
    case StoreCreated(store) =>
      storesById.get(store.id) match {
        case Some(exists) =>
          log.warning("ignoring StoreCreated notification for {}: store already exists", store.id)
        case None =>
          val sink = context.actorOf(Props[CassandraSink], "sink-" + store.id)
          storesById.put(store.id, store)
          storesByName.put(store.storeName, store)
          sinksByName.put(store.storeName, sink)
      }

    /* store event in the appropriate sink */
    case StoreEvent(store, event) =>
      for (sink <- sinksByName.get(store))
        sink ! event

    /* create a new query */
    case createQuery: CreateQuery =>
      storesByName.get(createQuery.store) match {
        case Some(store) =>
          val id = UUID.randomUUID()
          val query = context.actorOf(Props(new Query(id, createQuery, store)), "query-" + id.toString)
          context.watch(query)
          queries(id) = query
          log.debug("created query " + id)
          sender ! CreateQueryResponse(id)
        case None =>
      }

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

  case class StoreEvent(store: String, event: Event)

  /* query case classes */
  case object ListQueries
  case class ListQueriesResponse(queries: List[DescribeQueryResponse])
  case class CreateQuery(query: String, store: String, fields: Option[Set[String]], limit: Option[Int], reverse: Option[Boolean])
  case class CreateQueryResponse(id: UUID)
  case class GetEvents(id: UUID)
  case class DescribeQuery(id: UUID)
  case class DescribeQueryResponse(id: UUID, created: DateTime, state: String)
  case class DeleteQuery(id: UUID)

  /* store case classes */
  case object ListStores
  case class GetStoreFields()
  case class GetStoreFieldsResponse()
}