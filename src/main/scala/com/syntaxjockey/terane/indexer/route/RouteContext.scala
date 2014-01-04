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

import com.syntaxjockey.terane.indexer.{SinkMap, SourceRef}
import com.syntaxjockey.terane.indexer.bier.BierEvent

/**
 *
 */
case class RouteContext(matches: Vector[MatchStatement], action: MatchAction) {
  def process(source: SourceRef, sourceEvent: SourceEvent, sinks: SinkMap) {
    matches.map { matchStatement =>
      matchStatement.evaluate(sourceEvent, sinks) match {
        case Some(statementMatches) =>
          if (statementMatches)
            action.execute(sourceEvent.event, sinks)
          return
        case None => // do nothing
      }
    }
  }
}

/**
 *
 */
sealed trait MatchStatement {
  def evaluate(sourceEvent: SourceEvent, sinks: SinkMap): Option[Boolean]
}

object MatchesAll extends MatchStatement {
  def evaluate(sourceEvent: SourceEvent, sinks: SinkMap) = Some(true)
}

object MatchesNone extends MatchStatement {
  def evaluate(sourceEvent: SourceEvent, sinks: SinkMap) = Some(false)
}

class MatchesTag(tag: String) extends MatchStatement {
  def evaluate(sourceEvent: SourceEvent, sinks: SinkMap) = if (sourceEvent.event.tags.contains(tag)) Some(true) else None
}

/**
 *
 */
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
