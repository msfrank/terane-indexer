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
import spray.json.JsonParser
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._
import java.net.URLDecoder

import com.syntaxjockey.terane.indexer.SourceManager.{SourceManagerData, SourceManagerState}
import com.syntaxjockey.terane.indexer.source._
import com.syntaxjockey.terane.indexer.source.SourceSettings.SourceSettingsFormat
import com.syntaxjockey.terane.indexer.zookeeper.{Zookeeper, ZNode}

/**
 * The SourceManager is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for accepting events into the cluster and passing them to the EventRouter
 * for classification.
 */
class SourceManager(supervisor: ActorRef, eventRouter: ActorRef) extends Actor with ActorLogging with FSM[SourceManagerState,SourceManagerData] {
  import SourceManager._
  import context.dispatcher

  // config
  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client
  implicit val timeout = Timeout(10 seconds)

  // state
  var sources: Map[String,SourceRef] = Map.empty

  // listen for any broadcast operations
  context.system.eventStream.subscribe(self, classOf[SourceBroadcastOperation])

  // subscribe to leadership changes
  context.system.eventStream.subscribe(self, classOf[SupervisorEvent])

  startWith(Ready, WaitingForOperation)

  override def preStart() {
    /* ensure that /sources znode exists */
    if (zookeeper.checkExists().forPath("/sources") == null)
      zookeeper.create().forPath("/sources")
  }

  when(Ready) {
    /* load sinks from zookeeper synchronously */
    case Event(NodeBecomesLeader, _) =>
      sources = zookeeper.getChildren.forPath("/sources").map { child =>
        val path = "/sources/" + child
        val name = URLDecoder.decode(child, "UTF-8")
        val bytes = new String(zookeeper.getData.forPath(path), Zookeeper.UTF_8_CHARSET)
        val sourceSettings = SourceSettingsFormat.read(JsonParser(bytes))
        val sourceref = sourceSettings match {
          case settings: SyslogUdpSourceSettings =>
            SyslogUdpSource.open(zookeeper, name, settings, eventRouter)
          case settings: SyslogTcpSourceSettings =>
            SyslogTcpSource.open(zookeeper, name, settings, eventRouter)
        }
        log.debug("initialized source {}", name)
        name -> sourceref
      }.toMap
      goto(ClusterLeader)

    /* wait for the leader to give us the list of sinks */
    case Event(NodeBecomesWorker, _) =>
      goto(ClusterWorker)

  }

  onTransition {
    case Ready -> ClusterLeader =>
      context.system.eventStream.publish(SourceMap(sources))
  }

  when(ClusterLeader) {

    case Event(EnumerateSources, _) =>
      stay() replying EnumeratedSources(sources.values.toSeq)

    case Event(DescribeSource(name), _) =>
      val reply = sources.get(name) match {
        case Some(sink) =>
          sink
        case None =>
          ResourceNotFound
      }
      stay() replying reply

    /* if no operations are pending then execute, otherwise queue */
    case Event(op: SourceOperation, maybePending) =>
      val pendingOperations = maybePending match {
        case PendingOperations(current, queue) =>
          PendingOperations(current, queue :+ op -> sender)
        case WaitingForOperation =>
          executeOperation(op) pipeTo self
          PendingOperations(op -> sender, Vector.empty)
      }
      stay() using pendingOperations

    /* source has been created successfully */
    case Event(result @ CreatedSource(createOp, sourceref), PendingOperations((op, caller), queue)) =>
      caller ! result
      sources = sources + (createOp.name -> sourceref)
      context.system.eventStream.publish(SourceMap(sources))
      val pendingOperations = queue.headOption match {
        case Some((_op, _caller)) =>
          executeOperation(_op) pipeTo self
          PendingOperations(queue.head, queue.tail)
        case None =>
          WaitingForOperation
      }
      stay() using pendingOperations

    /* source has been deleted successfully */
    case Event(result @ DeletedSource(deleteOp), PendingOperations((op, caller), queue)) =>
      caller ! result
      sources = sources - deleteOp.name
      context.system.eventStream.publish(SourceMap(sources))
      val pendingOperations = queue.headOption match {
        case Some((_op, _caller)) =>
          executeOperation(_op) pipeTo self
          PendingOperations(queue.head, queue.tail)
        case None =>
          WaitingForOperation
      }
      stay() using pendingOperations

    /* an asynchronous operation failed */
    case Event(failure @ SourceOperationFailed(cause, failedOp), PendingOperations((op, caller), queue)) =>
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
    case Event(SourceBroadcastOperation(caller, op), _) =>
      self.tell(op, caller)
      stay()
  }

  when(ClusterWorker) {
    case Event(_, _) =>
      stay()
  }

  initialize()

  /**
   *
   */
  def executeOperation(op: SourceOperation): Future[SourceOperationResult] = {
    op match {
      case createOp: CreateSource =>
        createSource(createOp)
      case unknown =>
        Future.successful(SourceOperationFailed(new Exception("failed to execute %s".format(op)), op))
    }
  }

  /**
   *
   */
  def createSource(op: CreateSource): Future[SourceOperationResult] = {
    if (sources.contains(op.settings.name))
      Future.successful(SourceOperationFailed(new Exception("source %s already exists".format(op.settings.name)), op))
    else {
      Future {
        val sourceref = op.settings match {
          case settings: SyslogUdpSourceSettings =>
            SyslogUdpSource.create(zookeeper, op.name, settings, eventRouter)
          case settings: SyslogTcpSourceSettings =>
            SyslogTcpSource.create(zookeeper, op.name, settings, eventRouter)
        }
        log.debug("created source {}: {}", op.settings.name, op.settings)
        CreatedSource(op, sourceref)
      }.recover {
        case ex: Throwable =>
          SourceOperationFailed(ex, op)
      }
    }
  }
}

object SourceManager {
  def props(supervisor: ActorRef, eventRouter: ActorRef) = Props(classOf[SourceManager], supervisor, eventRouter)

  sealed trait SourceManagerState
  case object Ready extends SourceManagerState
  case object ClusterLeader extends SourceManagerState
  case object ClusterWorker extends SourceManagerState

  sealed trait SourceManagerData
  case object WaitingForOperation extends SourceManagerData
  case class PendingOperations(current: (SourceOperation,ActorRef), queue: Vector[(SourceOperation,ActorRef)]) extends SourceManagerData
}

case class Source(znode: ZNode, settings: SourceSettings)
case class SourceRef(actor: ActorRef, source: Source)
case class SourceMap(sources: Map[String,SourceRef])

sealed trait SourceOperationResult
sealed trait SourceCommandResult extends SourceOperationResult
sealed trait SourceQueryResult extends SourceOperationResult
case class SourceOperationFailed(cause: Throwable, op: SourceOperation) extends SourceOperationResult
case class SourceBroadcastOperation(caller: ActorRef, op: SourceOperation)

case class CreatedSource(op: CreateSource, result: SourceRef) extends SourceCommandResult
case class DeletedSource(op: DeleteSource) extends SourceCommandResult
case class EnumeratedSources(stores: Seq[SourceRef]) extends SourceQueryResult
