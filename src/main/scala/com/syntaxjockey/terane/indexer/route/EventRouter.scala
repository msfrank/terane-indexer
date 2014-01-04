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

package com.syntaxjockey.terane.indexer.route

import akka.actor._
import java.net.InetSocketAddress

import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer._
import com.syntaxjockey.terane.indexer.SinkMap
import com.syntaxjockey.terane.indexer.SourceMap
import com.syntaxjockey.terane.indexer.RouteMap

/**
 * The EventRouter is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for receiving events and routing them to the appropriate sink(s).
 * The routing policy is defined by a sequence of routes, which consist of one or
 * more match statements.  Each event is always compared against every route.
 *
 * the EventRouter actor is designed to be used with an akka router for higher
 * thoroughput.
 */
class EventRouter extends Actor with ActorLogging with Instrumented {

  // state
  var sourceMap = SourceMap(Map.empty)
  var sinkMap = SinkMap(Map.empty)
  var routeMap = RouteMap(Map.empty)

  // subscribe to RouteMap, SourceMap and SinkMap changes
  context.system.eventStream.subscribe(self, classOf[RouteMap])
  context.system.eventStream.subscribe(self, classOf[SourceMap])
  context.system.eventStream.subscribe(self, classOf[SinkMap])

  def receive = {

    /* the map of routes has changed */
    case _routeMap: RouteMap =>
      routeMap = _routeMap

    /* the map of sources has changed */
    case _sourceMap: SourceMap =>
      sourceMap = _sourceMap

    /* the map of sinks has changed */
    case _sinkMap: SinkMap =>
      sinkMap = _sinkMap

    /* send event to the appropriate sinks */
    case sourceEvent: SourceEvent =>
      sourceMap.sources.get(sourceEvent.source) match {
        case Some(sourceRef) =>
          routeMap.routes.values.foreach(_.context.process(sourceRef, sourceEvent, sinkMap))
        case None =>  // FIXME: retry at least once if source doesn't exist
      }
  }
}

object EventRouter {
  def props() = Props[EventRouter]
}

trait SourceEvent {
  val source: String
  val event: BierEvent
}

case class NetworkEvent(source: String, event: BierEvent, remote: InetSocketAddress, local: InetSocketAddress) extends SourceEvent
