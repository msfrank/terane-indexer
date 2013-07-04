package com.syntaxjockey.terane.indexer

import akka.actor._
import java.util.UUID
import org.joda.time.DateTime
import scala.Some
import scala.collection.JavaConversions._

import com.syntaxjockey.terane.indexer.metadata.{ZookeeperClient, StoreManager}
import com.syntaxjockey.terane.indexer.sink.{CassandraClient, CassandraSink}
import com.syntaxjockey.terane.indexer.bier.{Query, Event}
import com.typesafe.config.ConfigValueType

class EventRouter(zk: ZookeeperClient, cs: CassandraClient) extends Actor with ActorLogging {
  import EventRouter._
  import StoreManager._

  var storesById = Map.empty[String,Store]
  var storesByName = Map.empty[String,Store]
  val sinksByName = scala.collection.mutable.HashMap[String,ActorRef]()
  val queries = scala.collection.mutable.HashMap[UUID,ActorRef]()

  val storeManager = context.actorOf(Props(new StoreManager(zk, cs)), "store-manager")

  /* make sure all specified sinks have been created */
  if (context.system.settings.config.hasPath("terane.sinks"))
    context.system.settings.config.getConfig("terane.sinks").root()
      .filter { entry => entry._2.valueType() == ConfigValueType.OBJECT }
      .foreach { entry => storeManager ! CreateStore(entry._1) }

  log.debug("started {}", self.path.name)

  def receive = {

    /* a new store was created */
    case StoresChanged(_storesById, _storesByName) =>
      // remove any dropped stores
      storesById.values.filter(store => !_storesById.contains(store.id)).foreach { store =>
        for (sink <- sinksByName.remove(store.name)) {
          log.debug("terminating sink {} for store {}", sink.path.name, store.name)
          sink ! PoisonPill
        }
      }
      // add any created stores
      _storesById.values.filter(store => !storesById.contains(store.id)).foreach { store =>
        val keyspace = cs.getKeyspace(store.id)
        val sink = context.actorOf(Props(new CassandraSink(store, keyspace, zk)), "sink-" + store.id)
        sinksByName.put(store.name, sink)
        log.debug("creating sink {} for store {}", sink.path.name, store.name)
      }
      storesById = _storesById
      storesByName = _storesByName

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