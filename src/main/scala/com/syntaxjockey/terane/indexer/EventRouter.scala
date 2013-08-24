/**
 * Copyright 2013 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Terane.
 *
 * Terane is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Terane is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.syntaxjockey.terane.indexer

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import java.util.UUID
import org.joda.time.DateTime
import scala.Some
import scala.collection.JavaConversions._

import com.syntaxjockey.terane.indexer.metadata.{StoreManager}
import com.syntaxjockey.terane.indexer.sink.{CassandraSink}
import com.syntaxjockey.terane.indexer.bier.Event
import com.typesafe.config.ConfigValueType
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.cassandra.CassandraClient
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperClient

class EventRouter(zk: ZookeeperClient, cs: CassandraClient) extends Actor with ActorLogging {
  import EventRouter._
  import StoreManager._

  var storesById = Map.empty[String,Store]
  var storesByName = Map.empty[String,Store]
  val sinksByName = scala.collection.mutable.HashMap[String,ActorRef]()

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
      sinksByName.get(createQuery.store) match {
        case Some(sink) =>
          sink forward createQuery
        case None =>
          sender ! new Exception("no such store " + createQuery.store)
      }
  }
}

object EventRouter {
  case class StoreEvent(store: String, event: Event)
}