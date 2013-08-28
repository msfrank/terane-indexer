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

package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Actor, ActorLogging, LoggingFSM, Props}
import akka.actor.FSM.Normal
import akka.pattern.pipe
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnList
import org.joda.time.{DateTimeZone, DateTime}
import org.xbill.DNS.Name
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.Some
import scala.collection.JavaConversions._
import java.util.{Date, UUID}
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.{TickleParser, Matchers, Value, Event => BierEvent}
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, NoMoreMatches}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.Query.{Data, State}
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldsChanged
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store

class Query(id: UUID, createQuery: CreateQuery, store: Store, keyspace: Keyspace, fields: FieldsChanged) extends Actor with ActorLogging with LoggingFSM[State,Data] {
  import com.syntaxjockey.terane.indexer.bier.matchers._
  import Query._
  import context.dispatcher

  val created = DateTime.now(DateTimeZone.UTC)
  val reapingInterval = 30.seconds
  val maybeMatchers = TickleParser.buildMatchers(createQuery.query)

  // FIXME: if sorting is specified, using SortingStreamer
  val streamer = if (createQuery.sortBy.isDefined)
    context.actorOf(Props(new SortingStreamer(id, createQuery, fields)))
  else
    context.actorOf(Props(new DirectStreamer(id, createQuery, fields)))


  /* get term estimates and possibly reorder the query */
  maybeMatchers match {
    case Some(matchers) =>
      buildTerms(matchers, fields, keyspace) match {
        case Some(query) =>
          log.debug("parsed query '{}' => {}", createQuery.query, query)
          startWith(ReadingResults, ReadingResults(query, 0))
          query.nextPosting pipeTo self
        case None =>
          streamer ! NoMoreEvents
          setTimer("cancelling query", CancelQuery, reapingInterval, repeat = false)
          startWith(FinishedQuery, FinishedQuery(DateTime.now(DateTimeZone.UTC), 0))
      }
    case None =>
      streamer ! NoMoreEvents
      setTimer("cancelling query", CancelQuery, reapingInterval, repeat = false)
      startWith(FinishedQuery, FinishedQuery(DateTime.now(DateTimeZone.UTC), 0))
  }

  when(ReadingResults) {

    /**
     * The client has requested a query description.  Send a QueryStatistics object back.
     */
    case Event(DescribeQuery, _) =>
      stay() replying QueryStatistics(id, created, "Reading results", 0, 0)

    /**
     * The streamer is asking for the next event.  Request the next posting.
     */
    case Event(NextEvent, ReadingResults(query, numRead)) =>
      query.nextPosting pipeTo self
      stay()

    /**
     * The streamer has reached the query limit.
     */
    case Event(FinishedReading, ReadingResults(query, numRead)) =>
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), numRead)

    /**
     * The query has returned NoMoreMatches.
     */
    case Event(Left(NoMoreMatches), ReadingResults(query, numRead)) =>
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), numRead)

    /**
     * The query has returned a posting.  request the Event for this posting.
     */
    case Event(Right(BierPosting(eventId, _)), ReadingResults(query, numRead)) =>
      getEvent(eventId) pipeTo self
      stay()

    /**
     * The query has returned an event.  add the event to the events list.  if we haven't reached
     * the results limit for the query, then request the next event.
     */
    case Event(Success(event: BierEvent), ReadingResults(query, numRead)) =>
      streamer ! event
      stay() using ReadingResults(query, numRead + 1)

    /**
     *
     */
    case Event(Failure(ex), ReadingResults(query, numRead)) =>
      log.debug("failed to get event: {}", ex.getMessage)
      stay()

    /**
     * The client has requested the events, and we are still retrieving events.
     * transition to ProcessingRequest.
     */
    case Event(getEvents: GetEvents, ReadingResults(query, numRead)) =>
      streamer forward getEvents
      stay()

    /**
     * the client has requested to delete the query, and we are still retrieving events.
     * close the query and transition to FinishedQuery.
     */
    case Event(DeleteQuery, ReadingResults(query, numRead)) =>
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), numRead)
  }

  onTransition {
    case ReadingResults -> FinishedQuery => stateData match {
      case ReadingResults(query, _) =>
        streamer ! NoMoreEvents
        query.close()
        setTimer("cancelling query", CancelQuery, reapingInterval, repeat = false)
      case _ =>
        streamer ! NoMoreEvents
        setTimer("cancelling query", CancelQuery, reapingInterval, repeat = false)
    }
  }

  when(FinishedQuery) {

    /**
     * The client has requested a query description.  Send a QueryStatistics object back.
     */
    case Event(DescribeQuery, _) =>
      stay() replying QueryStatistics(id, created, "Finished query", 0, 0)

    case Event(getEvents: GetEvents, _) =>
      streamer forward getEvents
      stay()

    case Event(FinishedReading, _) =>
      stay()

    case Event(DeleteQuery, finishedQuery: FinishedQuery) =>
      stay()

    case Event(CancelQuery, finishedQuery: FinishedQuery) =>
      stop(Normal)
  }

  initialize()

  onTermination {
    case StopEvent(_, state, data) =>
      log.debug("deleted query {}", id)
  }

  /**
   * Recursively descend the Matchers tree replacing TermMatchers with Terms and removing
   * leaves and branches if they are not capable or returning any matches.
   *
   * @param matchers
   * @return
   */
  def buildTerms(matchers: Matchers, fields: FieldsChanged, keyspace: Keyspace): Option[Matchers] = {
    matchers match {
      case termMatcher @ TermMatcher(fieldId: FieldIdentifier, _) =>
        fields.fieldsByIdent.get(fieldId) match {
          case Some(field) =>
            termMatcher match {
              case TermMatcher(FieldIdentifier(_, DataType.TEXT), text: String) =>
                Some(new Term[String](fieldId, text, keyspace, field))
              case TermMatcher(FieldIdentifier(_, DataType.LITERAL), literal: String) =>
                Some(new Term[String](fieldId, literal, keyspace, field))
              case TermMatcher(FieldIdentifier(_, DataType.INTEGER), integer: Long) =>
                Some(new Term[Long](fieldId, integer, keyspace, field))
              case TermMatcher(FieldIdentifier(_, DataType.FLOAT), float: Double) =>
                Some(new Term[Double](fieldId, float, keyspace, field))
              case TermMatcher(FieldIdentifier(_, DataType.DATETIME), datetime: Date) =>
                Some(new Term[Date](fieldId, datetime, keyspace, field))
              case TermMatcher(FieldIdentifier(_, DataType.ADDRESS), address: Array[Byte]) =>
                Some(new Term[Array[Byte]](fieldId, address, keyspace, field))
              case TermMatcher(FieldIdentifier(_, DataType.HOSTNAME), hostname: String) =>
                Some(new Term[String](fieldId, hostname, keyspace, field))
              case unknown =>
                throw new Exception("unknown field type or value type for " + termMatcher.toString)
            }
          case missing =>
            None
        }
      case andGroup @ AndMatcher(children) =>
        AndMatcher(children map { child => buildTerms(child, fields, keyspace) } flatten) match {
          case AndMatcher(_children) if _children.isEmpty =>
            None
          case AndMatcher(_children) if _children.length == 1 =>
            Some(_children.head)
          case andMatcher: AndMatcher =>
            Some(andMatcher)
        }
      case orGroup @ OrMatcher(children) =>
        OrMatcher(children map { child => buildTerms(child, fields, keyspace) } flatten) match {
          case OrMatcher(_children) if _children.isEmpty =>
            None
          case OrMatcher(_children) if _children.length == 1 =>
            Some(_children.head)
          case orMatcher: OrMatcher =>
            Some(orMatcher)
        }
      case unknown =>
        throw new Exception("unknown Matcher type " + unknown.toString)
    }
  }

  /**
   * Asynchronously retrieve the specified events from cassandra.
   *
   * @param eventId
   * @return
   */
  def getEvent(eventId: UUID) = Future[Try[BierEvent]] {
    try {
      val result = keyspace.prepareQuery(CassandraSink.CF_EVENTS).getKey(eventId).execute()
      val columnList = result.getResult
      val event = readEvent(eventId, columnList)
      Success(event)
    } catch {
      case ex: Exception => Failure(ex)
    }
  }

  /**
   * Parse a cassandra row and return an Event.
   *
   * @param id
   * @param columnList
   * @return
   */
  def readEvent(id: UUID, columnList: ColumnList[String]): BierEvent = {
    import BierEvent._
    val values: Map[FieldIdentifier,Value] = columnList.filter(column => fields.fieldsByCf.contains(column.getName)).map { column =>
      fields.fieldsByCf(column.getName).fieldId match {
        case ident @ FieldIdentifier(_, DataType.TEXT) =>
          ident -> Value(text = Some(Text(column.getStringValue)))
        case ident @ FieldIdentifier(_, DataType.LITERAL) =>
          ident -> Value(literal = Some(Literal(column.getStringValue)))
        case ident @ FieldIdentifier(_, DataType.INTEGER) =>
          ident -> Value(integer = Some(Integer(column.getLongValue)))
        case ident @ FieldIdentifier(_, DataType.FLOAT) =>
          ident -> Value(float = Some(Float(column.getDoubleValue)))
        case ident @ FieldIdentifier(_, DataType.DATETIME) =>
          ident -> Value(datetime = Some(Datetime(new DateTime(column.getDateValue.getTime, DateTimeZone.UTC))))
        case ident @ FieldIdentifier(_, DataType.ADDRESS) =>
          ident -> Value(address = Some(Address(InetAddress.getByAddress(column.getByteArrayValue))))
        case ident @ FieldIdentifier(_, DataType.HOSTNAME) =>
          ident -> Value(hostname = Some(Hostname(Name.fromString(column.getStringValue))))
      }
    }.toMap
    new BierEvent(id, values)
  }
}

object Query {
  /* query case classes */
  case class GetEvents(offset: Option[Int] = None, limit: Option[Int] = None)
  case object NextEvent
  case object SendEvents
  case object DescribeQuery
  case object DeleteQuery
  case object CancelQuery
  case class EventSet(events: List[BierEvent], finished: Boolean)
  case class QueryStatistics(id: UUID, created: DateTime, state: String, numRead: Int, numSent: Int)
  case object NoMoreEvents
  case object FinishedReading

  /* our FSM states */
  sealed trait State
  case object WaitingForMatcherEstimates extends State
  case object ReadingResults extends State
  case object FinishedQuery extends State

  sealed trait Data
  case class ReadingResults(matchers: Matchers, numRead: Int) extends Data
  case class FinishedQuery(finished: DateTime, numRead: Int) extends Data
}
