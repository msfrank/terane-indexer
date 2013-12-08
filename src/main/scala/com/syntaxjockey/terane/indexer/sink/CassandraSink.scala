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

import akka.actor._
import akka.pattern.pipe
import akka.event.{SubchannelClassification, ActorEventBus}
import akka.util.Subclassification
import org.apache.curator.framework.CuratorFramework
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{StringSerializer, SetSerializer, UUIDSerializer}
import org.apache.cassandra.db.marshal.Int32Type
import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{BierEvent, FieldIdentifier}
import com.syntaxjockey.terane.indexer.cassandra.{CassandraKeyspaceOperations, Cassandra}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{State, Data}
import com.syntaxjockey.terane.indexer.{SinkManager, UUIDLike, RetryLater, Instrumented}
import com.syntaxjockey.terane.indexer.UUIDLike._

/**
 * CassandraSink is the parent actor for all activity for a particular sink,
 * such as querying and writing events.
 */
class CassandraSink(id: UUID, settings: CassandraSinkSettings, zookeeper: CuratorFramework) extends Actor with FSM[State,Data] with ActorLogging with CassandraKeyspaceOperations with Instrumented {
  import CassandraSink._
  import FieldManager._
  import StatsManager._
  import context.dispatcher

  // metrics
  val eventsReceived = metrics.meter("events-received", "events")
  val eventsWritten = metrics.meter("events-written", "events")
  val retriesOnce = metrics.meter("events-retried-once", "events")
  val retriesTwice = metrics.meter("events-retried-twice", "events")
  val retriesThrice = metrics.meter("events-retried-thrice", "events")
  val pathologicalRetries = metrics.meter("pathological-retries", "events")
  val eventBufferSize = metrics.counter("event-buffer-size")
  val queriesCreated = metrics.meter("queries-created", "queries")

  val cluster = Cassandra(context.system).cluster

  val sinkBus = new SinkBus()
  sinkBus.subscribe(self, classOf[FieldNotification])
  sinkBus.subscribe(self, classOf[StatsNotification])

  var currentFields = FieldMap(Map.empty, Map.empty)
  var currentStats = StatsMap(Map.empty)

  startWith(Unconnected, UnconnectedBuffer(Seq.empty))

  /* when unconnected we wait for command to create or get the keyspace */
  when(Unconnected) {
    case Event(SinkManager.Open, UnconnectedBuffer(retries)) =>
      Future(ConnectedToKeyspace(getKeyspace(UUIDLike(id)))).recover {case ex => SinkManager.Failure(ex)} pipeTo self
      goto(Connecting) using ConnectingBuffer(retries, Some(sender))
    case Event(SinkManager.Create, UnconnectedBuffer(retries)) =>
      Future(ConnectedToKeyspace(createKeyspace(UUIDLike(id)))).recover {case ex => SinkManager.Failure(ex)} pipeTo self
      goto(Connecting) using ConnectingBuffer(retries, Some(sender))
  }

  /* when connecting we buffer events until reconnection occurs */
  when(Connecting) {

    case Event(ConnectedToKeyspace(keyspace), ConnectingBuffer(retries, initiator)) =>
      val fields = context.actorOf(FieldManager.props(settings, keyspace, sinkBus), "field-manager")
      val stats = context.actorOf(StatsManager.props(settings, keyspace, sinkBus, fields), "stats-manager")
      val writers = context.actorOf(EventWriter.props(settings, keyspace, sinkBus, fields, stats), "writer")
      initiator.foreach(_ ! SinkManager.Done)
      goto(Connected) using EventBuffer(keyspace, writers, fields, stats, retries, None)

    /* getKeyspace or createKeyspace failed */
    case Event(failure: SinkManager.Failure, ConnectingBuffer(retries, initiator)) =>
      initiator.foreach(_ ! failure)
      goto(Unconnected) using UnconnectedBuffer(retries)

    case Event(fieldsChanged: FieldMap, _) =>
      currentFields = fieldsChanged
      stay()

    case Event(statsChanged: StatsMap, _) =>
      currentStats = statsChanged
      stay()

    case Event(event: BierEvent, UnconnectedBuffer(retries)) =>
      eventsReceived.mark()
      eventBufferSize.inc()
      stay() using UnconnectedBuffer(RetryEvent(event, 1) +: retries)

    case Event(retry: RetryEvent, UnconnectedBuffer(retries)) =>
      retry.attempt match {
        case 1 => retriesOnce.mark()
        case 2 => retriesTwice.mark()
        case 3 => retriesThrice.mark()
        case n => pathologicalRetries.mark()
      }
      eventBufferSize.inc()
      stay() using UnconnectedBuffer(retry +: retries)

    case Event(FlushRetries, _) =>
      stay()

    case Event(_: WroteEvent, _) =>
      eventsWritten.mark()
      stay()

    case Event(_: CreateQuery, _) =>
      stay() replying RetryLater
  }

  /* when connected we send events to the event writers */
  when(Connected) {

    case Event(fieldsChanged: FieldMap, _) =>
      currentFields = fieldsChanged
      stay()

    case Event(statsChanged: StatsMap, _) =>
      currentStats = statsChanged
      stay()

    case Event(event: BierEvent, eventBuffer: EventBuffer) =>
      eventsReceived.mark()
      eventBuffer.writers ! StoreEvent(event, 1)
      stay()

    case Event(retry: RetryEvent, EventBuffer(keyspace, writers, fields, stats, retries, scheduledFlush)) =>
      retry.attempt match {
        case 1 => retriesOnce.mark()
        case 2 => retriesTwice.mark()
        case 3 => retriesThrice.mark()
        case n => pathologicalRetries.mark()
      }
      eventBufferSize.inc()
      stay() using EventBuffer(keyspace, writers, fields, stats, retry +: retries, scheduledFlush)

    case Event(FlushRetries, EventBuffer(keyspace, writers, fields, stats, retries, _)) =>
      retries.foreach(retry => writers ! StoreEvent(retry.event, retry.attempt))
      val scheduledFlush = context.system.scheduler.scheduleOnce(settings.flushInterval, self, FlushRetries)
      eventBufferSize.dec(retries.length)
      stay() using EventBuffer(keyspace, writers, fields, stats, Seq.empty, Some(scheduledFlush))

    case Event(createQuery: CreateQuery, eventBuffer: EventBuffer) =>
      val id = UUID.randomUUID()
      context.system.actorOf(Query.props(id, createQuery, settings, eventBuffer.keyspace, currentFields, currentStats), "query-" + id.toString)
      queriesCreated.mark()
      sender ! CreatedQuery(id)
      stay()

    case Event(_: WroteEvent, _) =>
      eventsWritten.mark()
      stay()
  }

  initialize()

}

object CassandraSink {

  def props(id: UUID, settings: CassandraSinkSettings, zookeeper: CuratorFramework) = Props(classOf[CassandraSink], id, settings, zookeeper)

  val CF_EVENTS = new ColumnFamily[UUID,String]("events", UUIDSerializer.get(), StringSerializer.get())
  val SER_POSITIONS = new SetSerializer[java.lang.Integer](Int32Type.instance)

  case class ConnectedToKeyspace(keyspace: Keyspace)
  case class StoreEvent(event: BierEvent, attempt: Int)
  case class RetryEvent(event: BierEvent, attempt: Int)
  case class WroteEvent(event: BierEvent)
  case class WriteFailed(event: BierEvent)
  case object FlushRetries
  case class CreateQuery(query: String, store: String, fields: Option[Set[String]], sortBy: Option[List[FieldIdentifier]], limit: Option[Int], reverse: Option[Boolean])
  case class CreatedQuery(id: UUID)

  sealed trait State
  case object Unconnected extends State
  case object Connecting extends State
  case object Connected extends State

  sealed trait Data
  case class EventBuffer(keyspace: Keyspace, writers: ActorRef, fields: ActorRef, stats: ActorRef, events: Seq[RetryEvent], scheduledFlush: Option[Cancellable]) extends Data
  case class UnconnectedBuffer(events: Seq[RetryEvent]) extends Data
  case class ConnectingBuffer(events: Seq[RetryEvent], initiator: Option[ActorRef]) extends Data
}

/**
 * SinkBus is an event stream which is private to each individual CassandraSink, and
 * is used for broadcast communication between sink components.
 */
class SinkBus extends ActorEventBus with SubchannelClassification {
  type Event = SinkEvent
  type Classifier = Class[_]

  protected implicit val subclassification = new Subclassification[Class[_]] {
    def isEqual(x: Class[_], y: Class[_]) = x == y
    def isSubclass(x: Class[_], y: Class[_]) = y isAssignableFrom x
  }

  protected def classify(event: SinkEvent): Class[_] = event.getClass

  protected def publish(event: SinkEvent, subscriber: ActorRef) { subscriber ! event }
}

trait SinkEvent
