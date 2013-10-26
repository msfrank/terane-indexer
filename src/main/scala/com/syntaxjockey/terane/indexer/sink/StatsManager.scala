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

package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import akka.agent.Agent
import com.netflix.astyanax.Keyspace

import com.syntaxjockey.terane.indexer.metadata.Store
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics
import com.syntaxjockey.terane.indexer.zookeeper.Gossiper

/**
 * StatsManager handles store, field and posting statistics, which are used for
 * (among other things) calculation of sub-query costs in the query planner.
 */
class StatsManager(store: Store, val keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef) extends Actor with ActorLogging {
  import StatsManager._
  import FieldManager._
  import context.dispatcher

  val servicesPath = "/stores/" + store.name + "/services"
  val gossiper = context.actorOf(Gossiper.props("stats", servicesPath), "gossip")

  var currentStats = StatsMap(Map.empty)

  var fieldsByCf: Map[String,CassandraField] = Map.empty
  sinkBus.subscribe(self, classOf[FieldNotification])
  fieldManager ! GetFields

  def receive = {

    case FieldMap(_, fieldsChanged) =>
      fieldsByCf = fieldsChanged
      val statsAdded: Set[String] = fieldsByCf.keys.toSet[String] &~ currentStats.statsByCf.keys.toSet[String]
      val statsByCf = currentStats.statsByCf ++ statsAdded.map { id => id -> Agent(FieldStatistics.empty) }.toMap
      currentStats = StatsMap(statsByCf)
      sinkBus.publish(currentStats)
  }
}

object StatsManager {

  def props(store: Store, keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef) = {
    Props(classOf[StatsManager], store, keyspace, sinkBus, fieldManager)
  }

  case object GetStats

  sealed trait StatsEvent extends SinkEvent
  sealed trait StatsNotification extends StatsEvent
  case class StatsMap(statsByCf: Map[String,Agent[FieldStatistics]]) extends StatsNotification
}
