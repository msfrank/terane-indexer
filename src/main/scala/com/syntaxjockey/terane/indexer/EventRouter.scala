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
import java.net.InetAddress

/**
 * The EventRouter is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for receiving events and routing them to the appropriate sink(s).
 * The routing policy is defined by a sequence of routes, which consist of one or
 * more match statements.  Each event is always compared against every route.
 */
class EventRouter(supervisor: ActorRef) extends Actor with ActorLogging with Instrumented {

  val settings = IndexerConfig(context.system).settings

  // state
  var sourceMap = SourceMap(Map.empty)
  var sinkMap = SinkMap(Map.empty)
  var routes = Vector(Route("store-all", Vector(MatchesAll), StoreAllAction))

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

    /* send event to the appropriate sink */
    case StoreEvent(sourceName, event) =>
      routes.foreach(_.process(sourceMap.sources(sourceName), event, sinkMap))
  }
}

object EventRouter {
  def props(supervisor: ActorRef) = Props(classOf[EventRouter], supervisor)
}

case class StoreEvent(sourceName: String, event: BierEvent)

case class NetworkEvent(source: InetAddress, sourcePort: Int, dest: InetAddress, destPort: Int, event: BierEvent)

/**
 *
 */
case class Route(name: String, matches: Vector[MatchStatement], action: MatchAction) {
  def process(source: SourceRef, event: BierEvent, sinks: SinkMap) {
    matches.map { matchStatement =>
      matchStatement.evaluate(event, sinks) match {
        case Some(statementMatches) =>
          if (statementMatches)
            action.execute(event, sinks)
          return
        case None => // do nothing
      }
    }
  }
}

/* */
sealed trait MatchStatement {
  def evaluate(event: BierEvent, sinks: SinkMap): Option[Boolean]
}

object MatchesAll extends MatchStatement {
  def evaluate(event: BierEvent, sinks: SinkMap) = Some(true)
}

object MatchesNone extends MatchStatement {
  def evaluate(event: BierEvent, sinks: SinkMap) = Some(false)
}

class MatchesTag(tag: String) extends MatchStatement {
  def evaluate(event: BierEvent, sinks: SinkMap) = if (event.tags.contains(tag)) Some(true) else None
}

/* */
sealed trait MatchAction {
  def execute(event: BierEvent, sinks: SinkMap): Unit
}

case class StoreAction(targets: Vector[String]) extends MatchAction {
  def execute(event: BierEvent, sinks: SinkMap) {
    targets.foreach { target =>
      sinks.sinks.get(target) match {
        case Some(sink) =>
          sink.actor ! event
        case None =>  // do nothing
      }
    }
  }
}

object StoreAllAction extends MatchAction {
  def execute(event: BierEvent, sinks: SinkMap) {
    sinks.sinks.values.foreach(_.actor ! event)
  }
}

object DropAction extends MatchAction {
  def execute(event: BierEvent, sinks: SinkMap) { /* do nothing */ }
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

