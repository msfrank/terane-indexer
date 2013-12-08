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
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.twitter.algebird.{BF, HLL, CMS}
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.metadata.Store
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics
import com.syntaxjockey.terane.indexer.zookeeper.{Gossip, RequestGossip, Gossiper}
import com.syntaxjockey.terane.indexer.cassandra.{Serializers, MetaKey}
import scala.util.Random

/**
 * StatsManager handles store, field and posting statistics, which are used for
 * (among other things) calculation of sub-query costs in the query planner.
 */
class StatsManager(settings: CassandraSinkSettings, val keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef) extends Actor with ActorLogging {
  import StatsManager._
  import FieldManager._
  import context.dispatcher

  // config
  val servicesPath = "/stores/" + store.name + "/services"
  val nodeId = UUID.randomUUID()  // FIXME: get real node id
  val gossiper = context.actorOf(Gossiper.props("stats", servicesPath, 60.seconds), "gossip")
  val ttl = 0

  // state
  var currentStats = StatsMap(Map.empty)
  var fieldsByCf: Map[String,CassandraField] = Map.empty

  /* register to be notified of field changes */
  sinkBus.subscribe(self, classOf[FieldNotification])
  fieldManager ! GetFields

  def receive = {

    case FieldMap(_, fieldsChanged) =>
      fieldsByCf = fieldsChanged
      val statsAdded: Set[String] = fieldsByCf.keys.toSet[String] &~ currentStats.statsByCf.keys.toSet[String]
      val statsByCf = currentStats.statsByCf ++ statsAdded.map { id => id -> Agent(FieldStatistics.empty) }.toMap
      currentStats = StatsMap(statsByCf)
      sinkBus.publish(currentStats)
      statsAdded.foreach(fieldId => self ! MergeStats(fieldId))

    /* pick a random field to share with a peer */
    case RequestGossip =>
      if (!currentStats.statsByCf.isEmpty) {
        val statsSeq = currentStats.statsByCf.toSeq
        val (fieldId: String, agent: Agent[FieldStatistics]) = statsSeq(Random.nextInt(statsSeq.length))
        log.debug("picked field {} to gossip", fieldId)
        sender ! Gossip(StatGossip(fieldId, agent.get()))
      } else log.debug("ignoring gossip request, no stats to share")


    /* merge peer gossip with */
    case Gossip(StatGossip(fieldId, stats)) =>
      log.debug("received gossip about {}", fieldId)
      currentStats.statsByCf.get(fieldId) match {
        case Some(agent) =>
          agent.send(_ + stats)
        case None =>  // do nothing if we have no record of the fieldId
          log.debug("field {} is not known, ignoring gossip")
      }

    /* write stats to persistent storage */
    case WriteStats(fieldId) =>
      writeStats(fieldId)

    /* read stats from persistent storage */
    case MergeStats(fieldId) =>
      mergeStats(fieldId)
  }

  /**
   * write the field specified by fieldId to the meta column family.  if the field doesn't
   * exist, then do nothing.
   */
  def writeStats(fieldId: String) {
    currentStats.statsByCf.get(fieldId) match {
      case Some(agent) =>
        val mutation = keyspace.prepareMutationBatch()
        val row = mutation.withRow(StatsManager.CF_META, fieldId)
        val fieldStats = agent.get()
        row.putColumn(new MetaKey(TERM_FREQUENCIES, nodeId), writeTermFrequencies(fieldStats.termFrequencies), ttl)
        row.putColumn(new MetaKey(FIELD_CARDINALITY, nodeId), writeFieldCardinality(fieldStats.fieldCardinality), ttl)
        row.putColumn(new MetaKey(TERM_SET, nodeId), writeTermSet(fieldStats.termSet), ttl)
        row.putColumn(new MetaKey(TERM_COUNT, nodeId), writeTermCount(fieldStats.termCount), ttl)
        val result = mutation.execute()
        val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
        log.debug("wrote meta for {} in {}", fieldId, latency)
      case None =>  // do nothing if we have no record of the fieldId
    }
  }

  /**
   * merge the field specified by fieldId from the meta column family into the FieldStatistics agent.
   */
  def mergeStats(fieldId: String) {
    currentStats.statsByCf.get(fieldId) match {
      case Some(agent) =>
        val result = keyspace.prepareQuery(StatsManager.CF_META).getKey(fieldId).execute()
        val columnList = result.getResult
        val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
        var statsByNode: Map[UUID,Map[String,Array[Byte]]] = Map.empty
        columnList.getColumnNames.foreach { metaKey =>
          val nodeStats = statsByNode.getOrElse(metaKey.nodeId, Map.empty) ++ Map(metaKey.metaType -> columnList.getByteArrayValue(metaKey, Array.empty))
          statsByNode = statsByNode ++ Map(metaKey.nodeId -> nodeStats)
        }
        val fieldStats = FieldStatistics.merge(statsByNode.values.map { stats =>
          val termFrequencies = if (stats.contains(TERM_FREQUENCIES)) readTermFrequencies(stats(TERM_FREQUENCIES)) else FieldStatistics.empty.termFrequencies
          val fieldCardinality = if (stats.contains(FIELD_CARDINALITY)) readFieldCardinality(stats(FIELD_CARDINALITY)) else FieldStatistics.empty.fieldCardinality
          val termSet = if (stats.contains(TERM_SET)) readTermSet(stats(TERM_SET)) else FieldStatistics.empty.termSet
          val termCount = if (stats.contains(TERM_COUNT)) readTermCount(stats("term-count")) else FieldStatistics.empty.termCount
          FieldStatistics(termFrequencies, fieldCardinality, termSet, termCount, FieldStatistics.empty.fieldSignature)
        })
        agent.send(fieldStats)
        log.debug("merged meta for {} in {}", fieldId, latency)
      case None =>  // do nothing if we have no record of the fieldId
    }
  }

  /**
   * write out all known fields to the meta column family before stopping.
   */
  override def postStop() {
    currentStats.statsByCf.keys.foreach(writeStats)
  }
}

object StatsManager {

  def props(settings: CassandraSinkSettings, keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef) = {
    Props(classOf[StatsManager], settings, keyspace, sinkBus, fieldManager)
  }

  /**
   * deserialize a byte-array into a count-min sketch containing term frequencies
   */
  def readTermFrequencies(bytes: Array[Byte]): CMS = {
    val bstream = new ByteArrayInputStream(bytes)
    val istream = new ObjectInputStream(bstream)
    val termFrequencies = istream.readObject().asInstanceOf[CMS]
    istream.close()
    bstream.close()
    termFrequencies
  }

  /**
   * serialize term frequencies (a count-min sketch) into a byte-array
   */
  def writeTermFrequencies(termFrequencies: CMS): Array[Byte] = {
    val bstream = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(bstream)
    ostream.writeObject(termFrequencies)
    val bytes = bstream.toByteArray
    ostream.close()
    bstream.close()
    bytes
  }

  /**
   * deserialize a byte-array into a hyper-log-log containing the field cardinality
   */
  def readFieldCardinality(bytes: Array[Byte]): HLL = {
    val bstream = new ByteArrayInputStream(bytes)
    val istream = new ObjectInputStream(bstream)
    val fieldCardinality = istream.readObject().asInstanceOf[HLL]
    istream.close()
    bstream.close()
    fieldCardinality
  }

  /**
   * serialize field cardinality (a hyper-log-log) into a byte-array
   */
  def writeFieldCardinality(fieldCardinality: HLL): Array[Byte] = {
    val bstream = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(bstream)
    ostream.writeObject(fieldCardinality)
    val bytes = bstream.toByteArray
    ostream.close()
    bstream.close()
    bytes
  }

  /**
   * deserialize a byte-array into a bloom filter containing the term set
   */
  def readTermSet(bytes: Array[Byte]): BF = {
    val bstream = new ByteArrayInputStream(bytes)
    val istream = new ObjectInputStream(bstream)
    val termSet = istream.readObject().asInstanceOf[BF]
    istream.close()
    bstream.close()
    termSet
  }

  /**
   * serialize term set (a bloom filter) into a byte-array
   */
  def writeTermSet(termSet: BF): Array[Byte] = {
    val bstream = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(bstream)
    ostream.writeObject(termSet)
    val bytes = bstream.toByteArray
    ostream.close()
    bstream.close()
    bytes
  }

  /**
   * deserialize a byte-array into a long containing the term count
   */
  def readTermCount(bytes: Array[Byte]): Long = {
    val bstream = new ByteArrayInputStream(bytes)
    val istream = new ObjectInputStream(bstream)
    val termCount = istream.readLong()
    istream.close()
    bstream.close()
    termCount
  }

  /**
   * serialize term set (a bloom filter) into a byte-array
   */
  def writeTermCount(termCount: Long): Array[Byte] = {
    val bstream = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(bstream)
    ostream.writeLong(termCount)
    val bytes = bstream.toByteArray
    ostream.close()
    bstream.close()
    bytes
  }

  val CF_META = new ColumnFamily[String,MetaKey]("meta", StringSerializer.get(), Serializers.Meta)
  val TERM_FREQUENCIES = "term-frequencies"
  val FIELD_CARDINALITY = "field-cardinality"
  val TERM_SET = "term-set"
  val TERM_COUNT = "term-count"

  case object GetStats
  case class WriteStats(fieldId: String)
  case class MergeStats(fieldId: String)

  sealed trait StatsEvent extends SinkEvent
  sealed trait StatsNotification extends StatsEvent
  case class StatsMap(statsByCf: Map[String,Agent[FieldStatistics]]) extends StatsNotification
}

case class StatGossip(fieldId: String, stats: FieldStatistics)
