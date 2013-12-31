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
import scala.collection.JavaConversions._
import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers._
import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, NoMoreMatches}
import com.syntaxjockey.terane.indexer.sink.Query.{Data, State}
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldMap
import com.syntaxjockey.terane.indexer.sink.StatsManager.StatsMap
import com.syntaxjockey.terane.indexer.{CreateQuery,DeleteQuery,DescribeQuery,GetEvents}

/**
 * The Query actor manages the lifecycle of an individual query.  Query processing
 * consists of the following phases:
 *  1) parse the query string into a syntax tree.
 *  2) build a query plan from the syntax tree, incorporating store metadata and
 *     statistics to model the costs of each join.
 *  3) read each matching event and store it in temp file, possibly in sorted order if
 *     one or more sort fields are specified.
 *  4) fulfill GetEvents requests from the ApiService.
 *  5) eventually delete the temp file.
 */
class Query(id: UUID, createQuery: CreateQuery, settings: CassandraSinkSettings, keyspace: Keyspace, fields: FieldMap, stats: StatsMap) extends Actor with ActorLogging with LoggingFSM[State,Data] {
  import scala.language.postfixOps
  import Query._
  import context.dispatcher

  val created = DateTime.now(DateTimeZone.UTC)
  val reapingInterval = 30.seconds
  val parserParams = TickleParserParams("message")
  val maybeMatchers = TickleParser.buildMatchers(createQuery.query, parserParams)

  val streamer = if (createQuery.sortBy.isDefined)
    context.actorOf(SortingStreamer.props(id, createQuery, created, fields))
  else
    context.actorOf(DirectStreamer.props(id, createQuery, created, fields))

  /* get term estimates and possibly reorder the query */
  maybeMatchers match {
    case Some(matchers) =>
      buildTerms(matchers, keyspace, fields, stats) match {
        case Some(query) =>
          log.debug("query => {}'", createQuery.query)
          log.debug("matchers =>\n{}", prettyPrint(query))
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
      stay() replying QueryStatistics(id, created, "Reading results", 0, 0, getRuntime)

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
     * The client has requested a query description.  Forward to the streamer, which
     * will send a QueryStatistics message back.
     */
    case Event(DescribeQuery, _) =>
      streamer forward DescribeQuery
      stay()

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

  def getRuntime: Duration = Duration(new org.joda.time.Duration(created, DateTime.now(DateTimeZone.UTC)).getMillis, TimeUnit.MILLISECONDS)

  /**
   * Recursively descend the Matchers tree replacing TermMatchers with Terms and removing
   * leaves and branches if they are not capable or returning any matches.  Statistical information
   * is supplied to each matcher, so it can make query-planning decisions based on join costs.
   */
  def buildTerms(matchers: Matchers, keyspace: Keyspace, fields: FieldMap, stats: StatsMap): Option[Matchers] = {
    matchers match {

      case termMatcher @ TermMatcher(fieldId: FieldIdentifier, term: MatchTerm) =>
        fields.fieldsByIdent.get(fieldId) match {
          case Some(field) =>
            val stat = fieldId.fieldType match {
              case DataType.TEXT => stats.statsByCf.get(field.text.get.id)
              case DataType.LITERAL => stats.statsByCf.get(field.literal.get.id)
              case DataType.INTEGER => stats.statsByCf.get(field.integer.get.id)
              case DataType.FLOAT => stats.statsByCf.get(field.float.get.id)
              case DataType.DATETIME => stats.statsByCf.get(field.datetime.get.id)
              case DataType.ADDRESS => stats.statsByCf.get(field.address.get.id)
              case DataType.HOSTNAME => stats.statsByCf.get(field.hostname.get.id)
              case unknown =>
                throw new Exception("unknown field type or value type for " + termMatcher.toString)
            }
            Some(new Term(fieldId, term, keyspace, field, stat))
          case missing =>
            None
        }

      case rangeMatcher @ RangeMatcher(fieldId, spec: RangeSpec) =>
        fields.fieldsByIdent.get(fieldId) match {
          case Some(field) =>
            val stat = fieldId.fieldType match {
              case DataType.TEXT => stats.statsByCf.get(field.text.get.id)
              case DataType.LITERAL => stats.statsByCf.get(field.literal.get.id)
              case DataType.INTEGER => stats.statsByCf.get(field.integer.get.id)
              case DataType.FLOAT => stats.statsByCf.get(field.float.get.id)
              case DataType.DATETIME => stats.statsByCf.get(field.datetime.get.id)
              case DataType.ADDRESS => stats.statsByCf.get(field.address.get.id)
              case DataType.HOSTNAME => stats.statsByCf.get(field.hostname.get.id)
              case unknown =>
                throw new Exception("unknown field type or value type for " + rangeMatcher.toString)
            }
            Some(Range(fieldId, spec, keyspace, field, stat))
          case missing =>
            None
        }

      case _: EveryMatcher =>
        Some(Every(keyspace))

      case AndMatcher(children) =>
        val _children = children.map { child => buildTerms(child, keyspace, fields, stats) }.flatten
        if (_children.size == children.size)
          Some(AndMatcher(_children))
        else None

      case PhraseMatcher(children) =>
        val _children = children.map { child => buildTerms(child, keyspace, fields, stats) }.flatten
        if (_children.length == children.length)
          Some(PhraseMatcher(_children))
        else None

      case TermPlaceholder =>
        Some(TermPlaceholder)

      case OrMatcher(children) =>
        OrMatcher(children map { child => buildTerms(child, keyspace, fields, stats) } flatten) match {
          case OrMatcher(_children) if _children.isEmpty =>
            None
          case OrMatcher(_children) if _children.size == 1 =>
            Some(_children.head)
          case orMatcher: OrMatcher =>
            Some(orMatcher)
        }

      case NotMatcher(every: EveryMatcher, filter: Matchers) =>
        // FIXME: needs implementation
        throw new Exception("EveryMatcher is not implemented")

      case notMatcher: NotMatcher =>
        buildTerms(notMatcher.source, keyspace, fields, stats) match {
          case Some(source) =>
            buildTerms(notMatcher.filter, keyspace, fields, stats) match {
              case Some(filter) =>
                Some(new NotMatcher(source, filter))
              case None =>
                Some(source)
            }
          case None => None
        }

      case unknown =>
        throw new Exception("unknown Matcher type " + unknown.toString)
    }
  }

  /**
   * Asynchronously retrieve the specified events from cassandra.
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
   */
  def readEvent(id: UUID, columnList: ColumnList[String]): BierEvent = {
    val values: Map[FieldIdentifier,EventValue] = columnList.filter(column => fields.fieldsByCf.contains(column.getName)).map { column =>
      fields.fieldsByCf(column.getName).fieldId match {
        case ident @ FieldIdentifier(_, DataType.TEXT) =>
          ident -> EventValue(text = Some(Text(column.getStringValue)))
        case ident @ FieldIdentifier(_, DataType.LITERAL) =>
          ident -> EventValue(literal = Some(Literal(column.getStringValue)))
        case ident @ FieldIdentifier(_, DataType.INTEGER) =>
          ident -> EventValue(integer = Some(Integer(column.getLongValue)))
        case ident @ FieldIdentifier(_, DataType.FLOAT) =>
          ident -> EventValue(float = Some(Float(column.getDoubleValue)))
        case ident @ FieldIdentifier(_, DataType.DATETIME) =>
          ident -> EventValue(datetime = Some(Datetime(new DateTime(column.getDateValue.getTime, DateTimeZone.UTC))))
        case ident @ FieldIdentifier(_, DataType.ADDRESS) =>
          ident -> EventValue(address = Some(Address(InetAddress.getByAddress(column.getByteArrayValue))))
        case ident @ FieldIdentifier(_, DataType.HOSTNAME) =>
          ident -> EventValue(hostname = Some(Hostname(Name.fromString(column.getStringValue))))
      }
    }.toMap
    new BierEvent(id, values, Set.empty)
  }
}

object Query {

  def props(id: UUID, createQuery: CreateQuery, settings: CassandraSinkSettings, keyspace: Keyspace, fields: FieldMap, stats: StatsMap) = {
    Props(classOf[Query], id, createQuery, settings, keyspace, fields, stats)
  }

  /**
   * Return the string representation of a query matchers tree.
   */
  def prettyPrint(matcher: Matchers): String = {
    prettyPrintImpl(new StringBuilder(), matcher, 0).mkString
  }
  private def prettyPrintImpl(sb: StringBuilder, matcher: Matchers, indent: Int): StringBuilder = {
    matcher match {
      case term: TermMatcher =>
        sb.append(" " * indent)
        sb.append("%s=\"%s\"\n".format(term.fieldId, term.term.toString))
      case term: Term =>
        sb.append(" " * indent)
        sb.append("%s=\"%s\"\n".format(term.fieldId, term.term.toString))
      case range: RangeMatcher =>
        sb.append(" " * indent)
        sb.append("%s=%s\"%s\",\"%s\"%s\n".format(
          range.fieldId,
          if (range.spec.leftExcl) "{" else "[",
          range.spec.left.getOrElse("").toString,
          range.spec.right.getOrElse("").toString,
          if (range.spec.rightExcl) "}" else "]"))
      case range: Range =>
        sb.append(" " * indent)
        sb.append("%s=%s\"%s\",\"%s\"%s\n".format(
          range.fieldId,
          if (range.spec.leftExcl) "{" else "[",
          range.spec.left.getOrElse("").toString,
          range.spec.right.getOrElse("").toString,
          if (range.spec.rightExcl) "}" else "]"))
      case every: EveryMatcher =>
        sb.append(" " * indent)
        sb.append("EVERY\n")
      case every: Every =>
        sb.append(" " * indent)
        sb.append("EVERY\n")
      case AndMatcher(children) =>
        sb.append(" " * indent)
        sb.append("AND\n")
        children.foreach(prettyPrintImpl(sb, _, indent + 2))
      case PhraseMatcher(children) =>
        sb.append(" " * indent)
        sb.append("PHRASE\n")
        children.foreach(prettyPrintImpl(sb, _, indent + 2))
      case TermPlaceholder =>
        sb.append(" " * indent)
        sb.append("_\n")
      case OrMatcher(children) =>
        sb.append(" " * indent)
        sb.append("OR\n")
        children.foreach(prettyPrintImpl(sb, _, indent + 2))
      case NotMatcher(source, filter) =>
        sb.append(" " * indent)
        sb.append("FILTER\n")
        prettyPrintImpl(sb, filter, indent + 2)
        sb.append(" " * indent)
        sb.append("FROM\n")
        prettyPrintImpl(sb, source, indent + 2)
      case other =>
        sb.append(" " * indent)
        sb.append(other.toString)
    }
    sb
  }

  /* query case classes */
  case object NextEvent
  case object SendEvents
  case object CancelQuery
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

case class EventSet(fields: Map[String, FieldIdentifier], events: List[BierEvent], stats: QueryStatistics, finished: Boolean)

case class QueryStatistics(id: UUID, created: DateTime, state: String, numRead: Int, numSent: Int, runtime: Duration)
