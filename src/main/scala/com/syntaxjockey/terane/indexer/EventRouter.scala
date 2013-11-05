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
import com.typesafe.config.ConfigValueType
import scala.Some
import scala.collection.JavaConversions._

import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer.metadata.{Store, StoreManager}
import com.syntaxjockey.terane.indexer.sink.{CassandraSinkSettings, SinkSettings, CassandraSink}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery

class EventRouter extends Actor with ActorLogging with Instrumented {
  import EventRouter._
  import StoreManager._

  val storeManager = context.actorOf(Props[StoreManager], "store-manager")

  // state
  var storesById = Map.empty[String,Store]
  var storesByName = Map.empty[String,Store]
  val sinksByName = scala.collection.mutable.HashMap[String,ActorRef]()

  /* make sure all specified sinks have been created */
  IndexerConfig(context.system).settings.sinks.map { case (name: String, sinkSettings: SinkSettings) =>
    sinkSettings match {
      case cassandraSinkSettings: CassandraSinkSettings =>
        storeManager ! CreateStore(name)
    }
  }

  def receive = {

    /* a new store was created */
    case StoreMap(_storesById, _storesByName) =>
      // remove any dropped stores
      storesById.values.filter(store => !_storesById.contains(store.id)).foreach { store =>
        for (sink <- sinksByName.remove(store.name)) {
          log.debug("terminating sink {} for store {}", sink.path.name, store.name)
          sink ! PoisonPill
        }
      }
      // add any created stores
      _storesById.values.filter(store => !storesById.contains(store.id)).foreach { store =>
        val sink = context.actorOf(CassandraSink.props(store), "sink-" + store.id)
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

  def props() = Props[EventRouter]

  case class StoreEvent(store: String, event: BierEvent)
}