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
import akka.util.Timeout
import spray.json._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.util.UUID

import com.syntaxjockey.terane.indexer.SinkManager.{SinkManagerState,SinkManagerData}
import com.syntaxjockey.terane.indexer.sink.{CassandraSink, CassandraSinkSettings, SinkSettings}
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.sink.SinkSettings.SinkSettingsFormat

/**
 * The SinkManager manages sinks in a cluster, acting as a router for all sink operations.
 */
class SinkManager(supervisor: ActorRef) extends Actor with ActorLogging with FSM[SinkManagerState,SinkManagerData] {
  import SinkManager._
  import context.dispatcher

  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client

  implicit val timeout = Timeout(10 seconds)

  // state
  var sinks = SinkMap(Map.empty, Map.empty)
  var pending: Map[SinkOperation,ActorRef] = Map.empty

  context.system.eventStream.subscribe(self, classOf[SinkBroadcastOperation])

  override def preStart() {
    /* ensure that /sinks znode exists */
    if (zookeeper.checkExists().forPath("/sinks") == null)
      zookeeper.create().forPath("/sinks")
  }

  startWith(Ready, WaitingForData)

  when(Ready) {

    /* load sinks from zookeeper synchronously */
    case Event(NodeBecomesLeader, _) =>
      val sinksById: Map[UUID,SinkRef] = zookeeper.getChildren.forPath("/sinks").map { child =>
        val path = "/sinks/" + child
        val id = UUID.fromString(child)
        val value = JsonParser(new String(zookeeper.getData.forPath(path), Zookeeper.UTF_8_CHARSET))
        val sinkSettings = SinkSettingsFormat.read(value)
        val actor = sinkSettings match {
          case cassandraSinkSettings: CassandraSinkSettings =>
            context.actorOf(CassandraSink.props(id, cassandraSinkSettings, zookeeper.usingNamespace(path)))
        }
        // blocking on a future is ugly, but seems like the most straightforward way in this case
        Await.result(actor ? SinkManager.Create, 10 seconds) match {
          case SinkManager.Done =>  // do nothing
          case SinkManager.Failure(ex) => throw ex
        }
        log.debug("initialized sink %s: %s", id, sinkSettings)
        id -> SinkRef(actor, Sink(id, sinkSettings))
      }.toMap
      sinks = SinkMap(sinksById, sinksById.values.map(ref => ref.sink.settings.name -> ref).toMap)
      goto(ClusterLeader)

    /* ask the leader for the list of sinks */
    case Event(NodeBecomesWorker, _) =>
      supervisor ? LeaderOperation(self, EnumerateSinks) pipeTo self
      stay()

    case Event(EnumeratedSinks(sinkrefs), _) =>
      val sinksById = sinkrefs.map { sinkref =>
        val path = "/sinks/" + sinkref.sink.id.toString
        val sink = sinkref.sink
        val actor = sink.settings match {
          case cassandraSinkSettings: CassandraSinkSettings =>
            context.actorOf(CassandraSink.props(sink.id, cassandraSinkSettings, zookeeper.usingNamespace(path)))
        }
        // blocking on a future is ugly, but seems like the most straightforward way in this case
        Await.result(actor ? SinkManager.Open, 10 seconds) match {
          case SinkManager.Done =>  // do nothing
          case SinkManager.Failure(ex) => throw ex
        }
        log.debug("initialized sink %s: %s", sink.id, sink.settings)
        sink.id -> SinkRef(actor, sink)
      }.toMap
      val sinksByName = sinksById.values.map(ref => ref.sink.settings.name -> ref).toMap
      sinks = SinkMap(sinksById, sinksByName)
      goto(ClusterWorker)
  }

  when(ClusterLeader) {

    /* create a new sink, if it doesn't exist already */
    case Event(op: CreateSink, _) =>
      val id = UUID.randomUUID()
      if (pending.contains(op)) {
        sender ! SinkOperationFailed(new Exception("%s is already in progress".format(op)), op)
      } else if (sinks.sinksByName.contains(op.settings.name)) {
        sender ! SinkOperationFailed(new Exception("sink %s already exists".format(op.settings.name)), op)
      } else {
        pending = pending + (op -> sender)
        createSink(op, id) pipeTo self
      }
      stay()

    /* sink has been created successfully */
    case Event(result @ CreatedSink(createSink, sinkref), _) =>
      pending.get(createSink) match {
        case Some(caller) =>
          caller ! result
          pending = pending - createSink
        case None =>  // FIXME: do we do anything here?
      }
      val sink = sinkref.sink
      val SinkMap(sinksById, sinksByName) = sinks
      sinks = SinkMap(sinksById + (sink.id -> sinkref), sinksByName + (sink.settings.name -> sinkref))
      stay()

    /* delete a sink, if it exists */
    case Event(deleteSink: DeleteSink, _) =>
      stay() replying SinkOperationFailed(new NotImplementedError("DeleteSink not implemented"), deleteSink)

    /* describe a sink, if it exists */
    case Event(describeSink: DescribeSink, _) =>
      stay() replying SinkOperationFailed(new NotImplementedError("DescribeSink not implemented"), describeSink)

    /* describe a sink if it exists */
    case Event(findSink: FindSink, _) =>
      stay() replying SinkOperationFailed(new NotImplementedError("FindSink not implemented"), findSink)

    /* list all known sinks */
    case Event(EnumerateSinks, _) =>
      stay() replying EnumeratedSinks(sinks.sinksById.values.toSeq)

    /* an asynchronous operation failed */
    case Event(failure @ SinkOperationFailed(cause, op), _) =>
      log.error("operation {} failed: {}", op, cause.getMessage)
      pending.get(op) match {
        case Some(caller) =>
          caller ! failure
          pending = pending - op
        case None =>  // FIXME: do we do anything here?
      }
      stay()

    /* forward the operation to self on behalf of caller */
    case Event(SinkBroadcastOperation(caller, op), _) =>
      self.tell(op, caller)
      stay()
  }

  /**
   *
   */
  def createSink(op: CreateSink, id: UUID): Future[SinkOperationResult] = Future {
    val path = "/sinks/" + id.toString
    // FIXME: lock znode
    val actor = op.settings match {
      case cassandraSinkSettings: CassandraSinkSettings =>
        zookeeper.create().forPath(path, op.settings.toJson.prettyPrint.getBytes)
        context.actorOf(CassandraSink.props(id, cassandraSinkSettings, zookeeper.usingNamespace(path)))
    }
    val sink = Sink(id, op.settings)
    val sinkref = SinkRef(actor, sink)
    // blocking on a future is ugly, but seems like the most straightforward way in this case
    Await.result(actor ? SinkManager.Create, 10 seconds) match {
      case SinkManager.Done =>  // do nothing
      case SinkManager.Failure(ex) => throw ex
    }
    log.debug("created sink %s (%s): %s", op.settings.name, id, op.settings)
    CreatedSink(op, sinkref)
  }.recover {
    // FIXME: perform cleanup if any part of creation fails
    case ex: Throwable =>
      SinkOperationFailed(ex, op)
  }

}

object SinkManager {

  def props(supervisor: ActorRef) = Props(classOf[SinkManager], supervisor)

  case object Open
  case object Create
  case object Done
  case class Failure(ex: Throwable)

  sealed trait SinkManagerState
  case object Ready extends SinkManagerState
  case object ClusterLeader extends SinkManagerState
  case object ClusterWorker extends SinkManagerState

  sealed trait SinkManagerData
  case object WaitingForData extends SinkManagerData
}

case class Sink(id: UUID, settings: SinkSettings)
case class SinkRef(actor: ActorRef, sink: Sink)
case class SinkMap(sinksById: Map[UUID,SinkRef], sinksByName: Map[String,SinkRef])

sealed trait SinkOperationResult
sealed trait SinkCommandResult extends SinkOperationResult
sealed trait SinkQueryResult extends SinkOperationResult
case class SinkOperationFailed(cause: Throwable, op: SinkOperation) extends SinkOperationResult
case class SinkBroadcastOperation(caller: ActorRef, op: SinkOperation)

case class CreatedSink(op: CreateSink, result: SinkRef) extends SinkCommandResult
case class DeletedSink(op: DeleteSink, result: SinkRef) extends SinkCommandResult
case class EnumeratedSinks(sinks: Seq[SinkRef]) extends SinkQueryResult
