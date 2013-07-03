package com.syntaxjockey.terane.indexer.sink

import akka.actor._
import scala.concurrent.duration._
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{StringSerializer, ListSerializer, SetSerializer, UUIDSerializer}
import org.apache.cassandra.db.marshal.{UTF8Type, Int32Type}
import java.util.UUID

import com.syntaxjockey.terane.indexer.sink.CassandraSink.{State, Data}
import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent}
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.metadata.ZookeeperClient

/**
 *
 */
class CassandraSink(store: Store, keyspace: Keyspace, zk: ZookeeperClient) extends Actor with FSM[State,Data] with ActorLogging {
  import CassandraSink._
  import FieldManager._

  import context.dispatcher
  val config = context.system.settings.config.getConfig("terane.cassandra")

  val fieldBus = new FieldBus()
  val fieldManager = context.actorOf(Props(new FieldManager(store, keyspace, zk, fieldBus)), "field-manager")
  val writers = context.actorOf(Props(new EventWriter(store, keyspace, fieldManager)), "writer")
  fieldBus.subscribe(writers, classOf[FieldNotification])
  //val readers = context.actorOf(Props(new EventSearcher(store, keyspace, fieldManager)), "reader")
  //fieldBus.subscribe(readers, classOf[FieldNotification])

  startWith(Connected, EventBuffer(Seq.empty, None))
  self ! FlushRetries

  log.debug("started {}", self.path.name)

  /* when unconnected we buffer events until reconnection occurs */
  when(Unconnected) {
    case Event(event: BierEvent, UnconnectedBuffer(retries)) =>
      stay() using UnconnectedBuffer(RetryEvent(event, 1) +: retries)
    case Event(retry: RetryEvent, UnconnectedBuffer(retries)) =>
      stay() using UnconnectedBuffer(retry +: retries)
    case Event(FlushRetries, _) =>
      stay()
    case Event(Connected, UnconnectedBuffer(retries)) =>
      self ! FlushRetries
      goto(Connected) using EventBuffer(retries, None)
  }

  /* when connected we send events to the event writers */
  when(Connected) {
    case Event(event: BierEvent, EventBuffer(retries, scheduledFlush)) =>
      writers ! StoreEvent(event, 1)
      stay()
    case Event(retry: RetryEvent, EventBuffer(retries, scheduledFlush)) =>
      stay() using EventBuffer(retry +: retries, scheduledFlush)
    case Event(FlushRetries, EventBuffer(retries, _)) =>
      retries.foreach(retry => writers ! StoreEvent(retry.event, retry.attempt))
      val scheduledFlush = context.system.scheduler.scheduleOnce(FLUSH_INTERVAL, self, FlushRetries)
      stay() using EventBuffer(Seq.empty, Some(scheduledFlush))
  }

  initialize()
}

object CassandraSink {
  val FLUSH_INTERVAL = 60.seconds
  val CF_EVENTS = new ColumnFamily[UUID,String]("events", UUIDSerializer.get(), StringSerializer.get())
  val SER_POSITIONS = new SetSerializer[java.lang.Integer](Int32Type.instance)
  val SER_LITERAL = new ListSerializer[java.lang.String](UTF8Type.instance)

  case class StoreEvent(event: BierEvent, attempt: Int)
  case class RetryEvent(event: BierEvent, attempt: Int)
  case object FlushRetries

  sealed trait State
  case object Unconnected extends State
  case object Connected extends State

  sealed trait Data
  case class EventBuffer(events: Seq[RetryEvent], scheduledFlush: Option[Cancellable]) extends Data
  case class UnconnectedBuffer(events: Seq[RetryEvent]) extends Data
}
