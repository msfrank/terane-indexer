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

import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer.EventRouter.StoreEvent

/**
 * The EventRouter is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for receiving events and routing them to the appropriate sink(s).
 * The routing policy is defined by a sequence of routes, which consist of one or
 * more match statements.  Each event is always compared against every route.
 */
class EventRouter(supervisor: ActorRef) extends Actor with ActorLogging with Instrumented {

  val settings = IndexerConfig(context.system).settings

  // state
  var sinkMap = SinkMap(Map.empty)

  // subscribe to leadership changes
  context.system.eventStream.subscribe(self, classOf[SupervisorEvent])

  // subscribe to SinkMap changes
  context.system.eventStream.subscribe(self, classOf[SinkMap])

  override def preStart() {
    /* create any locally-defined routes */

    /* get the current sinks */
    context.system.eventStream.publish(SinkBroadcastOperation(self, EnumerateSinks))
  }

  def receive = {

    case NodeBecomesLeader =>

    case NodeBecomesWorker =>

    /* the map of sinks has changed */
    case _sinkMap: SinkMap =>
      sinkMap = _sinkMap

    /* send event to the appropriate sink */
    case StoreEvent(sourceName, event) =>
      sinkMap.sinks.values.foreach(sink => sink.actor ! event)
  }
}

object EventRouter {

  def props(supervisor: ActorRef) = Props(classOf[EventRouter], supervisor)

  case class StoreEvent(sourceName: String, event: BierEvent)
}

case class Route(id: String, name: String, target: Sink, matches: Seq[MatchStatement])

sealed trait MatchStatement
object MatchesAll extends MatchStatement {
  def apply(event: BierEvent) = true
}
class MatchesTag(tag: String) {
  def apply(event: BierEvent): Boolean = event.tags.contains(tag)
}


/**
 * abstract base class for all API failures.
 */
abstract class ApiFailure(val description: String)

/**
 * trait and companion object for API failures which indicate the operation
 * should be retried at a later time.
 */
trait RetryLater
case object RetryLater extends ApiFailure("retry operation later") with RetryLater

/**
 * trait and companion object for API failures which indicate the operation
 * parameters must be modified before being submitted again.
 */
trait BadRequest
case object BadRequest extends ApiFailure("bad request") with BadRequest

/**
 * trait and companion object for API failures which indicate the resource
 * was not found.
 */
trait ResourceNotFound
case object ResourceNotFound extends ApiFailure("resource not found") with ResourceNotFound

/**
 * Exception which wraps an API failure.
 */
class ApiException(val failure: ApiFailure) extends Exception(failure.description)

