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
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{StringSerializer, SetSerializer, UUIDSerializer}
import org.apache.cassandra.db.marshal.Int32Type
import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent}
import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.metadata.Store
import com.syntaxjockey.terane.indexer.cassandra.{CassandraKeyspaceOperations, Cassandra}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{State, Data}
import com.syntaxjockey.terane.indexer.http.RetryLater

/**
 *
 */
class CassandraSink(store: Store) extends Actor with FSM[State,Data] with ActorLogging with CassandraKeyspaceOperations {
  import CassandraSink._
  import FieldManager._
  import context.dispatcher

  val config = context.system.settings.config.getConfig("terane.cassandra")
  val flushInterval = Duration(config.getMilliseconds("flush-interval"), TimeUnit.MILLISECONDS)

  val cluster = Cassandra(context.system).cluster
  val keyspace = getKeyspace(store.id)

  var currentFields = FieldMap(Map.empty, Map.empty)
  val fieldBus = new FieldBus()
  fieldBus.subscribe(self, classOf[FieldNotification])

  val fieldManager = context.actorOf(Props(new FieldManager(store, keyspace, fieldBus)), "field-manager")

  val writers = context.actorOf(Props(new EventWriter(store, keyspace, fieldManager)), "writer")
  fieldBus.subscribe(writers, classOf[FieldNotification])

  startWith(Connected, EventBuffer(Seq.empty, None))
  self ! FlushRetries

  log.debug("started {}", self.path.name)

  /* when unconnected we buffer events until reconnection occurs */
  when(Unconnected) {

    case Event(fieldsChanged: FieldMap, _) =>
      currentFields = fieldsChanged
      stay()

    case Event(event: BierEvent, UnconnectedBuffer(retries)) =>
      stay() using UnconnectedBuffer(RetryEvent(event, 1) +: retries)

    case Event(retry: RetryEvent, UnconnectedBuffer(retries)) =>
      stay() using UnconnectedBuffer(retry +: retries)

    case Event(FlushRetries, _) =>
      stay()

    case Event(Connected, UnconnectedBuffer(retries)) =>
      self ! FlushRetries
      goto(Connected) using EventBuffer(retries, None)

    // FIXME: add metric
    case Event(_: WroteEvent, _) =>
      stay()

    case Event(_: CreateQuery, _) =>
      stay() replying RetryLater
  }

  /* when connected we send events to the event writers */
  when(Connected) {

    case Event(fieldsChanged: FieldMap, _) =>
      currentFields = fieldsChanged
      stay()

    case Event(event: BierEvent, EventBuffer(retries, scheduledFlush)) =>
      writers ! StoreEvent(event, 1)
      stay()

    case Event(retry: RetryEvent, EventBuffer(retries, scheduledFlush)) =>
      stay() using EventBuffer(retry +: retries, scheduledFlush)

    case Event(FlushRetries, EventBuffer(retries, _)) =>
      retries.foreach(retry => writers ! StoreEvent(retry.event, retry.attempt))
      val scheduledFlush = context.system.scheduler.scheduleOnce(flushInterval, self, FlushRetries)
      stay() using EventBuffer(Seq.empty, Some(scheduledFlush))

    case Event(createQuery: CreateQuery, _) =>
      val id = UUID.randomUUID()
      context.system.actorOf(Props(new Query(id, createQuery, store, keyspace, currentFields)), "query-" + id.toString)
      sender ! CreatedQuery(id)
      stay()

    // FIXME: add metric
    case Event(_: WroteEvent, _) =>
      stay()
  }

  initialize()
}

object CassandraSink {

  val CF_EVENTS = new ColumnFamily[UUID,String]("events", UUIDSerializer.get(), StringSerializer.get())
  val CF_META = new ColumnFamily[UUID,String]("meta", UUIDSerializer.get(), StringSerializer.get())
  val SER_POSITIONS = new SetSerializer[java.lang.Integer](Int32Type.instance)

  case class StoreEvent(event: BierEvent, attempt: Int)
  case class RetryEvent(event: BierEvent, attempt: Int)
  case class WroteEvent(event: BierEvent)
  case class WriteFailed(event: BierEvent)
  case object FlushRetries
  case class CreateQuery(query: String, store: String, fields: Option[Set[String]], sortBy: Option[List[FieldIdentifier]], limit: Option[Int], reverse: Option[Boolean])
  case class CreatedQuery(id: UUID)

  sealed trait State
  case object Unconnected extends State
  case object Connected extends State

  sealed trait Data
  case class EventBuffer(events: Seq[RetryEvent], scheduledFlush: Option[Cancellable]) extends Data
  case class UnconnectedBuffer(events: Seq[RetryEvent]) extends Data
}
