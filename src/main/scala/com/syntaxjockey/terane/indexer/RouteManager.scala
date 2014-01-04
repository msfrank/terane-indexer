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
import scala.concurrent.duration._

import com.syntaxjockey.terane.indexer.zookeeper.{Zookeeper, ZNode}
import com.syntaxjockey.terane.indexer.route.{RouteContext, RouteSettings}

/**
 * The RouteManager is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for creating, deleting, and modifying routes, and notifying the
 * eventRouter when changes occur.
 */
class RouteManager(supervisor: ActorRef, eventRouter: ActorRef) extends Actor with ActorLogging with Instrumented {

  // config
  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client
  implicit val timeout = Timeout(10 seconds)

  // state
  var sourceMap = SourceMap(Map.empty)
  var sinkMap = SinkMap(Map.empty)
  var routeMap = RouteMap(Map.empty)

  // subscribe to SourceMap and SinkMap changes
  context.system.eventStream.subscribe(self, classOf[SourceMap])
  context.system.eventStream.subscribe(self, classOf[SinkMap])

  def receive = {

    /* the map of sources has changed */
    case _sourceMap: SourceMap =>
      sourceMap = _sourceMap

    /* the map of sinks has changed */
    case _sinkMap: SinkMap =>
      sinkMap = _sinkMap

  }
}

object RouteManager {
  def props(supervisor: ActorRef, eventRouter: ActorRef) = Props(classOf[RouteManager], supervisor, eventRouter)
}


case class Route(znode: ZNode, settings: RouteSettings)
case class RouteRef(context: RouteContext, route: Route)
case class RouteMap(routes: Map[String,RouteRef])

sealed trait RouteOperationResult
sealed trait RouteCommandResult extends RouteOperationResult
sealed trait RouteQueryResult extends RouteOperationResult
case class RouteOperationFailed(cause: Throwable, op: RouteOperation) extends RouteOperationResult
case class RouteBroadcastOperation(caller: ActorRef, op: RouteOperation)

case class CreatedRoute(op: CreateRoute, result: RouteRef) extends RouteCommandResult
case class DeletedRoute(op: DeleteRoute) extends RouteCommandResult
case class EnumeratedRoutes(sinks: Seq[RouteRef]) extends RouteQueryResult