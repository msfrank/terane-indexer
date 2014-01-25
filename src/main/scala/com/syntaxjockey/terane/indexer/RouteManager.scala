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
import akka.util.Timeout
import akka.pattern.pipe
import spray.json.JsonParser
import org.apache.zookeeper.data.Stat
import scala.concurrent.Future
import scala.concurrent.duration._

import com.syntaxjockey.terane.indexer.RouteManager.{RouteManagerData, RouteManagerState}
import com.syntaxjockey.terane.indexer.zookeeper.{Zookeeper, ZNode}
import com.syntaxjockey.terane.indexer.route.RoutingChain
import com.syntaxjockey.terane.indexer.route.RouteSettings.RoutingChainFormat

/**
 * The RouteManager is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for creating, deleting, and modifying routes, and notifying the
 * eventRouter when changes occur.
 */
class RouteManager(supervisor: ActorRef, eventRouter: ActorRef) extends Actor with ActorLogging with FSM[RouteManagerState,RouteManagerData] {
  import RouteManager._
  import context.dispatcher

  // config
  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client
  implicit val timeout = Timeout(10 seconds)

  // state
  var sourceMap = SourceMap(Map.empty)
  var sinkMap = SinkMap(Map.empty)
  var routes: RoutingTable = RoutingTable(ZNode.empty, RoutingChain.empty)

  // listen for any broadcast operations
  context.system.eventStream.subscribe(self, classOf[SinkBroadcastOperation])

  // subscribe to SourceMap and SinkMap changes
  context.system.eventStream.subscribe(self, classOf[SourceMap])
  context.system.eventStream.subscribe(self, classOf[SinkMap])

  // subscribe to leadership changes
  context.system.eventStream.subscribe(self, classOf[SupervisorEvent])

  override def preStart() {
    /* ensure that /sinks znode exists */
    val ensure = zookeeper.newNamespaceAwareEnsurePath("/routes")
    ensure.ensure(zookeeper.getZookeeperClient)
  }

  startWith(Ready, WaitingForOperation)

  when(Ready) {

    /* the map of sources has changed */
    case Event(_sourceMap: SourceMap, _) =>
      sourceMap = _sourceMap
      stay()

    /* the map of sinks has changed */
    case Event(_sinkMap: SinkMap, _) =>
      sinkMap = _sinkMap
      stay()

    /* load sinks from zookeeper synchronously */
    case Event(NodeBecomesLeader, _) =>
      val stat = new Stat()
      val bytes = new String(zookeeper.getData.storingStatIn(stat).forPath("/routes"), Zookeeper.UTF_8_CHARSET)
      routes = if (bytes.trim.length > 0)
        RoutingTable(stat, RoutingChainFormat.read(JsonParser(bytes)))
      else
        RoutingTable(ZNode.empty, RoutingChain.empty)
      goto(ClusterLeader)

    /* wait for the leader to give us the list of sinks */
    case Event(NodeBecomesWorker, _) =>
      goto(ClusterWorker)

  }

  onTransition {
    case Ready -> ClusterLeader =>
      context.system.eventStream.publish(routes)
  }

  when(ClusterLeader) {

    case Event(EnumerateRoutes, _) =>
      stay() replying EnumeratedRoutes(routes)

    /* if no operations are pending then execute, otherwise queue */
    case Event(op: RouteOperation, maybePending) =>
      val pendingOperations = maybePending match {
        case PendingOperations(current, queue) =>
          PendingOperations(current, queue :+ op -> sender)
        case WaitingForOperation =>
          executeOperation(op) pipeTo self
          PendingOperations(op -> sender, Vector.empty)
      }
      stay() using pendingOperations

    /* routing table has been modified successfully */
    case Event(result: RouteCommandResult, PendingOperations((op, caller), queue)) =>
      caller ! result
      routes = result.result
      context.system.eventStream.publish(routes)
      val pendingOperations = queue.headOption match {
        case Some((_op, _caller)) =>
          executeOperation(_op) pipeTo self
          PendingOperations(queue.head, queue.tail)
        case None =>
          WaitingForOperation
      }
      stay() using pendingOperations

    /* an asynchronous operation failed */
    case Event(failure @ RouteOperationFailed(cause, failedOp), PendingOperations((op, caller), queue)) =>
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
    case Event(RouteBroadcastOperation(caller, op), _) =>
      self.tell(op, caller)
      stay()
  }

  /**
   *
   */
  def executeOperation(op: RouteOperation): Future[RouteOperationResult] = {
    val future = op match {
      case _op @ CreateRoute(route, None) =>
        executeModification(routes.root.append(route), routes.znode).map(CreatedRoute(_op, _))
      case _op @ CreateRoute(route, Some(position)) =>
        executeModification(routes.root.insert(position, route), routes.znode).map(CreatedRoute(_op, _))
      case _op @ ReplaceRoute(route, position) =>
        executeModification(routes.root.replace(position, route), routes.znode).map(ReplacedRoute(_op, _))
      case _op @ DeleteRoute(None) =>
        executeModification(routes.root.flush(), routes.znode).map(DeletedRoute(_op, _))
      case _op @ DeleteRoute(Some(position)) =>
        executeModification(routes.root.delete(position), routes.znode).map(DeletedRoute(_op, _))
      case EnumerateRoutes =>
        Future.successful(EnumeratedRoutes(routes))
    }
    future.recover {
      case ex: Throwable => RouteOperationFailed(ex, op)
    }
  }

  /**
   *
   */
  def executeModification(routes: RoutingChain, znode: ZNode): Future[RoutingTable] = Future {
    val bytes = RoutingChainFormat.write(routes).prettyPrint.getBytes(Zookeeper.UTF_8_CHARSET)
    val stat = zookeeper.setData().withVersion(znode.version).forPath("/routes", bytes)
    RoutingTable(stat, routes)
  }

  initialize()
}

object RouteManager {

  def props(supervisor: ActorRef, eventRouter: ActorRef) = Props(classOf[RouteManager], supervisor, eventRouter)

  sealed trait RouteManagerState
  case object Ready extends RouteManagerState
  case object ClusterLeader extends RouteManagerState
  case object ClusterWorker extends RouteManagerState

  sealed trait RouteManagerData
  case object WaitingForOperation extends RouteManagerData
  case class PendingOperations(current: (RouteOperation,ActorRef), queue: Vector[(RouteOperation,ActorRef)]) extends RouteManagerData
}

case class RoutingTable(znode: ZNode, root: RoutingChain)

sealed trait RouteOperationResult
sealed trait RouteCommandResult extends RouteOperationResult { val result: RoutingTable }
sealed trait RouteQueryResult extends RouteOperationResult
case class RouteOperationFailed(cause: Throwable, op: RouteOperation) extends RouteOperationResult
case class RouteBroadcastOperation(caller: ActorRef, op: RouteOperation)

case class CreatedRoute(op: CreateRoute, result: RoutingTable) extends RouteCommandResult
case class ReplacedRoute(op: ReplaceRoute, result: RoutingTable) extends RouteCommandResult
case class DeletedRoute(op: DeleteRoute, result: RoutingTable) extends RouteCommandResult
case class EnumeratedRoutes(result: RoutingTable) extends RouteQueryResult