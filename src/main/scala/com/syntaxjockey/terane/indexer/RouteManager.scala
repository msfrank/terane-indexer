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
import scala.collection.JavaConversions._
import java.net.{URLEncoder, URLDecoder}

import com.syntaxjockey.terane.indexer.RouteManager.{RouteManagerData, RouteManagerState}
import com.syntaxjockey.terane.indexer.zookeeper.{Zookeeper, ZNode}
import com.syntaxjockey.terane.indexer.route.{RouteContext, RouteSettings}
import com.syntaxjockey.terane.indexer.route.RouteSettings.RouteContextFormat

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
  var routes: Map[String,Route] = Map.empty

  // listen for any broadcast operations
  context.system.eventStream.subscribe(self, classOf[SinkBroadcastOperation])

  // subscribe to SourceMap and SinkMap changes
  context.system.eventStream.subscribe(self, classOf[SourceMap])
  context.system.eventStream.subscribe(self, classOf[SinkMap])

  // subscribe to leadership changes
  context.system.eventStream.subscribe(self, classOf[SupervisorEvent])

  override def preStart() {
    /* ensure that /sinks znode exists */
    if (zookeeper.checkExists().forPath("/routes") == null)
      zookeeper.create().forPath("/routes")
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
      routes = zookeeper.getChildren.forPath("/routes").map { child =>
        val path = "/routes/" + child
        val name = URLDecoder.decode(child, "UTF-8")
        val stat = new Stat()
        val bytes = new String(zookeeper.getData.storingStatIn(stat).forPath(path), Zookeeper.UTF_8_CHARSET)
        val routeContext = RouteContextFormat.read(JsonParser(bytes))
        log.debug("initialized route {}", name)
        name -> Route(stat, routeContext)
      }.toMap
      goto(ClusterLeader)

    /* wait for the leader to give us the list of sinks */
    case Event(NodeBecomesWorker, _) =>
      goto(ClusterWorker)

  }

  onTransition {
    case Ready -> ClusterLeader =>
      context.system.eventStream.publish(RouteMap(routes))
  }

  when(ClusterLeader) {

    case Event(EnumerateRoutes, _) =>
      stay() replying EnumeratedRoutes(routes.values.toSeq)

    case Event(DescribeRoute(name), _) =>
      val reply = routes.get(name) match {
        case Some(route) =>
          route
        case None =>
          ResourceNotFound
      }
      stay() replying reply

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

    /* route has been created successfully */
    case Event(result @ CreatedRoute(createOp, route), PendingOperations((op, caller), queue)) =>
      caller ! result
      routes = routes + (createOp.context.name -> route)
      context.system.eventStream.publish(RouteMap(routes))
      val pendingOperations = queue.headOption match {
        case Some((_op, _caller)) =>
          executeOperation(_op) pipeTo self
          PendingOperations(queue.head, queue.tail)
        case None =>
          WaitingForOperation
      }
      stay() using pendingOperations

    /* route has been deleted successfully */
    case Event(result @ DeletedRoute(deleteOp), PendingOperations((op, caller), queue)) =>
      caller ! result
      routes = routes - deleteOp.name
      context.system.eventStream.publish(RouteMap(routes))
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
    op match {
      case createOp: CreateRoute =>
        createRoute(createOp)
      case deleteOp: DeleteRoute =>
        deleteRoute(deleteOp)
      case unknown =>
        Future.successful(RouteOperationFailed(new Exception("failed to execute %s".format(op)), op))
    }
  }

  /**
   *
   */
  def createRoute(op: CreateRoute): Future[RouteOperationResult] = {
    if (routes.contains(op.context.name))
      Future.successful(RouteOperationFailed(new Exception("route %s already exists".format(op.context.name)), op))
    else {
      Future {

        val path = "/routes/" + URLEncoder.encode(op.context.name, "UTF-8")
        val bytes = RouteSettings.RouteContextFormat.write(op.context).prettyPrint.getBytes(Zookeeper.UTF_8_CHARSET)
        zookeeper.create().forPath(path, bytes)
        val stat = zookeeper.checkExists().forPath(path)
        val route = Route(stat, op.context)
        log.debug("created route {}", op.context)
        CreatedRoute(op, route)
      }.recover {
        case ex: Throwable =>
          RouteOperationFailed(ex, op)
      }
    }
  }

  /**
   *
   */
  def deleteRoute(op: DeleteRoute): Future[RouteOperationResult] = {
    if (!routes.contains(op.name))
      Future.successful(RouteOperationFailed(new Exception("route %s does not exist".format(op.name)), op))
    else {
      Future.successful(DeletedRoute(op))
    }
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


case class Route(znode: ZNode, context: RouteContext)
case class RouteMap(routes: Map[String,Route])

sealed trait RouteOperationResult
sealed trait RouteCommandResult extends RouteOperationResult
sealed trait RouteQueryResult extends RouteOperationResult
case class RouteOperationFailed(cause: Throwable, op: RouteOperation) extends RouteOperationResult
case class RouteBroadcastOperation(caller: ActorRef, op: RouteOperation)

case class CreatedRoute(op: CreateRoute, result: Route) extends RouteCommandResult
case class DeletedRoute(op: DeleteRoute) extends RouteCommandResult
case class EnumeratedRoutes(sinks: Seq[Route]) extends RouteQueryResult