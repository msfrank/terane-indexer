/*
 * *
 *  * Copyright (c) 2010-${YEAR} Michael Frank <msfrank@syntaxjockey.com>
 *  *
 *  * This file is part of Terane.
 *  *
 *  * Terane is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Terane is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.syntaxjockey.terane.indexer

import akka.actor._
import akka.pattern.pipe
import akka.pattern.ask
import org.joda.time.DateTime
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{FieldIdentifier, BierEvent}
import akka.util.Timeout

/**
 * SearchManager keeps track of queries, and acts as a proxy between the API and the
 * sinks.
 */
class SearchManager(supervisor: ActorRef) extends Actor with ActorLogging {
  import SearchManager._
  import context.dispatcher

  // config
  implicit val timeout = Timeout(10.seconds)

  // state
  var sinkMap = SinkMap(Map.empty)
  var queriesByRef: Map[ActorRef,UUID] = Map.empty
  var queriesById: Map[UUID,SearchRef] = Map.empty

  // subscribe to SinkMap changes
  context.system.eventStream.subscribe(self, classOf[SinkMap])
  //context.system.eventStream.subscribe(self, classOf[SupervisorEvent])

  def receive = {

    /* the map of sinks has changed */
    case _sinkMap: SinkMap =>
      sinkMap = _sinkMap

    /* create a query searching the specified sink */
    case op: CreateQuery =>
      sinkMap.sinks.get(op.store) match {
        case None =>
          sender ! SearchOperationFailed(new Exception("sink %s doesn't exist".format(op.store)), op)
        case Some(sinkref) =>
          log.debug("searching {} using params {}", op.store, op)
          val caller = sender   // capture sender reference
          sinkref.actor.ask(op).map {
            case result: SearchOperationResult => SearchOperationResultWithCaller(result, caller)
          } pipeTo self
      }

    /* created a new query */
    case SearchOperationResultWithCaller(result: CreatedQuery, caller) =>
      queriesById = queriesById + (result.result.search.id -> result.result)
      queriesByRef = queriesByRef + (result.result.actor -> result.result.search.id)
      context.watch(result.result.actor)
      caller ! result

    /* delete the query matching the specified id */
    case op: DeleteQuery =>
      queriesById.get(op.id) match {
        case None =>
          sender ! SearchOperationFailed(new Exception("query %s doesn't exist".format(op.id)), op)
        case Some(searchref) =>
          searchref.actor ! PoisonPill
          sender ! DeletedQuery(op)
      }

    /* list all currently executing queries */
    case EnumerateQueries =>
      sender ! queriesById.values.toSeq

    case DescribeQuery(id) =>

    /* an operation failed */
    case SearchOperationResultWithCaller(failure: SearchOperationFailed, caller) =>
      caller ! failure

    /* stop tracking query */
    case Terminated(query) =>
      queriesByRef.get(query) match {
        case Some(id) =>
          queriesByRef = queriesByRef - query
          if (queriesById.contains(id))
            queriesById = queriesById - id
        case None =>  // do nothing
      }
  }

}

object SearchManager {

  def props(supervisor: ActorRef) = Props(classOf[SearchManager], supervisor)

  case class SearchOperationResultWithCaller(result: SearchOperationResult, caller: ActorRef)
}

sealed trait SearchOperationResult
sealed trait SearchCommandResult extends SearchOperationResult
sealed trait SearchQueryResult extends SearchOperationResult
case class SearchOperationFailed(cause: Throwable, op: SearchOperation) extends SearchOperationResult

case class Search(id: UUID, params: CreateQuery)
case class SearchRef(actor: ActorRef, search: Search)

case class CreatedQuery(op: CreateQuery, result: SearchRef) extends SearchCommandResult
case class DeletedQuery(op: DeleteQuery) extends SearchCommandResult
case class EnumeratedSearches(queries: Seq[SearchRef]) extends SearchQueryResult
case class EventSet(fields: Map[String, FieldIdentifier], events: List[BierEvent], stats: QueryStatistics, finished: Boolean) extends SearchQueryResult
case class QueryStatistics(id: UUID, created: DateTime, state: String, numRead: Int, numSent: Int, runtime: Duration) extends SearchQueryResult
