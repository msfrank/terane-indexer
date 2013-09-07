package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import com.netflix.astyanax.Keyspace

import com.syntaxjockey.terane.indexer.metadata.Store
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics
import akka.agent.Agent

/**
 * StatsManager handles store, field and posting statistics, which are used for
 * (among other things) calculation of sub-query costs in the query planner.
 *
 * @param store
 * @param keyspace
 * @param sinkBus
 * @param fieldManager
 */
class StatsManager(store: Store, val keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef) extends Actor with ActorLogging {
  import StatsManager._
  import FieldManager._
  import context.dispatcher

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
