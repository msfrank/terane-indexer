package com.syntaxjockey.terane.indexer.sink

import akka.actor._
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{StringSerializer, ListSerializer, SetSerializer, UUIDSerializer}
import org.apache.cassandra.db.marshal.{UTF8Type, Int32Type}
import java.util.UUID

import com.syntaxjockey.terane.indexer.sink.CassandraSink.{State, Data}
import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent}
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.metadata.ZookeeperClient
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldBus

/**
 *
 */
class CassandraSink(store: Store, keyspace: Keyspace, zk: ZookeeperClient) extends Actor with FSM[State,Data] with ActorLogging {
  import CassandraSink._
  import context.dispatcher

  val config = context.system.settings.config.getConfig("terane.cassandra")

  val fieldBus = new FieldBus()
  val fieldManager = context.actorOf(Props(new FieldManager(store, keyspace, zk, fieldBus)), "field-manager")

  val writers = context.actorOf(Props(new EventWriter(store, keyspace, fieldBus)), "writer")
  //val readers = context.actorOf()

  startWith(Connected, EventBuffer(Seq.empty))

  log.debug("started {}", self.path.name)

  /* when unconnected we buffer events until reconnection occurs */
  when(Unconnected) {
    case Event(cs: CassandraClient, EventBuffer(retries)) =>
      retries.foreach(retry => self ! retry)
      goto(Connected) using EventBuffer(Seq.empty)
    case Event(event: BierEvent, EventBuffer(retries)) =>
      stay() using EventBuffer(RetryEvent(event, 1) +: retries)
    case Event(retry: RetryEvent, EventBuffer(retries)) =>
      stay() using EventBuffer(retry +: retries)
  }

  /* when connected we send events to the event writers */
  when(Connected) {
    case Event(event: BierEvent, EventBuffer(retries)) =>
      writers ! StoreEvent(event, 1)
      stay()
    case Event(retry: RetryEvent, EventBuffer(retries)) =>
      writers ! StoreEvent(retry.event, retry.attempt)
      stay()
  }

  initialize()
}

object CassandraSink {
  val CF_EVENTS = new ColumnFamily[UUID,String]("events", UUIDSerializer.get(), StringSerializer.get())
  val SER_POSITIONS = new SetSerializer[java.lang.Integer](Int32Type.instance)
  val SER_LITERAL = new ListSerializer[java.lang.String](UTF8Type.instance)

  case class StoreEvent(event: BierEvent, attempt: Int)
  case class RetryEvent(event: BierEvent, attempt: Int)

  sealed trait State
  case object Unconnected extends State
  case object Connected extends State

  sealed trait Data
  case class EventBuffer(events: Seq[RetryEvent]) extends Data
}
