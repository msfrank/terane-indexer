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
import akka.pattern.pipe
import akka.util.Timeout
import spray.json._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.util.UUID
import java.net.URLDecoder

import com.syntaxjockey.terane.indexer.SinkManager.{SinkManagerState,SinkManagerData}
import com.syntaxjockey.terane.indexer.sink.{CassandraSink, CassandraSinkSettings, SinkSettings}
import com.syntaxjockey.terane.indexer.zookeeper.{ZNode, Zookeeper}
import com.syntaxjockey.terane.indexer.sink.SinkSettings.SinkSettingsFormat

/**
 * The SinkManager manages sinks in a cluster, acting as a router for all sink operations.
 */
class SinkManager(supervisor: ActorRef) extends Actor with ActorLogging with FSM[SinkManagerState,SinkManagerData] {
  import SinkManager._
  import context.dispatcher

  // config
  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client
  implicit val timeout = Timeout(10 seconds)

  // state
  var sinks: Map[String,SinkRef] = Map.empty

  // listen for any broadcast operations
  context.system.eventStream.subscribe(self, classOf[SinkBroadcastOperation])

  // subscribe to leadership changes
  context.system.eventStream.subscribe(self, classOf[SupervisorEvent])

  override def preStart() {
    /* ensure that /sinks znode exists */
    if (zookeeper.checkExists().forPath("/sinks") == null)
      zookeeper.create().forPath("/sinks")
  }

  startWith(Ready, WaitingForOperation)

  when(Ready) {

    /* load sinks from zookeeper synchronously */
    case Event(NodeBecomesLeader, _) =>
      sinks = zookeeper.getChildren.forPath("/sinks").map { child =>
        val path = "/sinks/" + child
        val name = URLDecoder.decode(child, "UTF-8")
        val bytes = new String(zookeeper.getData.forPath(path), Zookeeper.UTF_8_CHARSET)
        val sinkSettings = SinkSettingsFormat.read(JsonParser(bytes))
        val sinkref = sinkSettings match {
          case settings: CassandraSinkSettings =>
            CassandraSink.open(zookeeper, name, settings)
        }
        log.debug("initialized sink %s", name)
        name -> sinkref
      }.toMap
      goto(ClusterLeader)

    /* wait for the leader to give us the list of sinks */
    case Event(NodeBecomesWorker, _) =>
      goto(ClusterWorker)

  }

  onTransition {
    case Ready -> ClusterLeader =>
      context.system.eventStream.publish(SinkMap(sinks))
  }

  when(ClusterLeader) {

    /* if no operations are pending then execute, otherwise queue */
    case Event(op: SinkOperation, maybePending) =>
      val pendingOperations = maybePending match {
        case PendingOperations(current, queue) =>
          PendingOperations(current, queue :+ op -> sender)
        case WaitingForOperation =>
          executeOperation(op) pipeTo self
          PendingOperations(op -> sender, Vector.empty)
      }
      stay() using pendingOperations

    /* sink has been created successfully */
    case Event(result @ CreatedSink(createOp, sinkref), PendingOperations((op, caller), queue)) =>
      caller ! result
      sinks = sinks + (createOp.name -> sinkref)
      context.system.eventStream.publish(SinkMap(sinks))
      val pendingOperations = queue.headOption match {
        case Some((_op, _caller)) =>
          executeOperation(_op) pipeTo self
          PendingOperations(queue.head, queue.tail)
        case None =>
          WaitingForOperation
      }
      stay() using pendingOperations

    /* an asynchronous operation failed */
    case Event(failure @ SinkOperationFailed(cause, failedOp), PendingOperations((op, caller), queue)) =>
      log.error("operation {} failed: {}", failedOp, cause.getMessage)
      caller ! failure
      val pendingOperations = queue.headOption match {
        case Some((_op, _caller)) =>
          executeOperation(_op) pipeTo self
          PendingOperations(queue.head, queue.tail)
        case None =>
          WaitingForOperation
      }
      stay() using pendingOperations

    /* forward the operation to self on behalf of caller */
    case Event(SinkBroadcastOperation(caller, op), _) =>
      self.tell(op, caller)
      stay()
  }

  when(ClusterWorker) {

    case Event(SinkMap(_sinks), _) =>
      val sinksAdded = _sinks.filter { case (name,_) => !sinks.contains(name) }
        .map { case (name,ref) => name -> ref.sink }
      val sinksUpdated = _sinks.filter { case (name,ref) => sinks.contains(name) && ref.sink.znode.mzxid > sinks(name).sink.znode.mzxid }
        .map { case (name,ref) => name -> ref.sink }
      val sinksRemoved = sinks.filter { case (name,_) => !_sinks.contains(name) }
      goto(ClusterWorker)
  }

  initialize()

  /**
   *
   */
  def executeOperation(op: SinkOperation): Future[SinkOperationResult] = {
    op match {
      case createOp: CreateSink =>
        createSink(createOp)
      case deleteOp: DeleteSink =>
        deleteSink(deleteOp)
      case unknown =>
        Future.successful(SinkOperationFailed(new Exception("failed to execute %s".format(op)), op))
    }
  }

  /**
   *
   */
  def createSink(op: CreateSink): Future[SinkOperationResult] = {
    if (sinks.contains(op.settings.name))
      Future.successful(SinkOperationFailed(new Exception("sink %s already exists".format(op.settings.name)), op))
    else {
      Future {
        val sinkref = op.settings match {
          case settings: CassandraSinkSettings =>
            CassandraSink.create(zookeeper, op.name, settings)
        }
        log.debug("created sink %s: %s", op.settings.name, op.settings)
        CreatedSink(op, sinkref)
      }.recover {
        case ex: Throwable =>
          SinkOperationFailed(ex, op)
      }
    }
  }

  /**
   *
   */
  def deleteSink(op: DeleteSink): Future[SinkOperationResult] = {
    if (!sinks.contains(op.name))
      Future.successful(SinkOperationFailed(new Exception("sink %s does not exist".format(op.name)), op))
    else {
      Future.successful(DeletedSink(op))
    }
  }
}

object SinkManager {

  def props(supervisor: ActorRef) = Props(classOf[SinkManager], supervisor)

  sealed trait SinkManagerState
  case object Ready extends SinkManagerState
  case object ClusterLeader extends SinkManagerState
  case object ClusterWorker extends SinkManagerState

  sealed trait SinkManagerData
  case object WaitingForOperation extends SinkManagerData
  case class PendingOperations(current: (SinkOperation,ActorRef), queue: Vector[(SinkOperation,ActorRef)]) extends SinkManagerData
}

case class Sink(id: UUID, znode: ZNode, settings: SinkSettings)
case class SinkRef(actor: ActorRef, sink: Sink)
case class SinkMap(sinks: Map[String,SinkRef])

sealed trait SinkOperationResult
sealed trait SinkCommandResult extends SinkOperationResult
sealed trait SinkQueryResult extends SinkOperationResult
case class SinkOperationFailed(cause: Throwable, op: SinkOperation) extends SinkOperationResult
case class SinkBroadcastOperation(caller: ActorRef, op: SinkOperation)

case class CreatedSink(op: CreateSink, result: SinkRef) extends SinkCommandResult
case class DeletedSink(op: DeleteSink) extends SinkCommandResult
case class EnumeratedSinks(sinks: Seq[SinkRef]) extends SinkQueryResult
