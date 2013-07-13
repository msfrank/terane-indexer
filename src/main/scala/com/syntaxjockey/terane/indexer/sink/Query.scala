package com.syntaxjockey.terane.indexer.sink

import scala.concurrent.Future
import scala.Some
import scala.collection.JavaConversions._
import akka.actor._
import akka.pattern.pipe
import com.netflix.astyanax.Keyspace
import org.joda.time.{DateTimeZone, DateTime}
import com.netflix.astyanax.model.ColumnList
import org.xbill.DNS.Name
import java.util.UUID
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.sink.Query.{Data, State}
import com.syntaxjockey.terane.indexer.bier.{TickleParser, EventValueType, Matchers}
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, NoMoreMatches}
import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent}
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldsChanged
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery

class Query(id: UUID, createQuery: CreateQuery, store: Store, keyspace: Keyspace, fields: FieldsChanged) extends Actor with ActorLogging with LoggingFSM[State,Data] {
  import com.syntaxjockey.terane.indexer.bier.matchers._
  import Query._
  import context.dispatcher

  val created = DateTime.now(DateTimeZone.UTC)
  val limit = createQuery.limit.getOrElse(100)
  val matchers = TickleParser.buildMatchers(createQuery.query)

  /* get term estimates and possibly reorder the query */
  if (matchers.isDefined) {
    buildTerms(matchers.get, fields, keyspace) match {
      case Some(query) =>
        startWith(WaitingForRequest, WaitingForRequest(query, List.empty, 0))
        query.nextPosting pipeTo self
      case None =>
        startWith(FinishedQuery, EmptyQuery)
    }
  } else startWith(FinishedQuery, EmptyQuery)

  when(WaitingForRequest) {

    case Event(DescribeQuery, _) =>
      sender ! QueryStatistics(id, created, "Waiting for client request")
      stay()

    case Event(Left(NoMoreMatches), WaitingForRequest(query, events, numFound)) =>
      stay() using FinishedRequest(events)

    case Event(Right(BierPosting(eventId, _)), WaitingForRequest(query, events, numFound)) =>
      getEvents(List(eventId)) pipeTo self
      stay()

    case Event(FoundEvents(foundEvents), WaitingForRequest(query, events, numFound)) =>
      if (numFound + foundEvents.length < limit)
        query.nextPosting pipeTo self
      stay() using WaitingForRequest(query, events ++ foundEvents, numFound + foundEvents.length)

    case Event(GetEvents, WaitingForRequest(query, events, numFound)) =>
      goto(ProcessingRequest) using ProcessingRequest(sender, query, events, numFound)

    case Event(GetEvents, FinishedRequest(events)) =>
      self ! SendEvents
      goto(SendingRequest) using SendingRequest(sender, events)
  }

  when(ProcessingRequest) {

    case Event(DescribeQuery, _) =>
      sender ! QueryStatistics(id, created, "Processing client request")
      stay()

    case Event(Left(NoMoreMatches), ProcessingRequest(client, query, events, numFound)) =>
      self ! SendEvents
      goto(SendingRequest) using SendingRequest(client, events)

    case Event(Right(BierPosting(eventId, _)), ProcessingRequest(client, query, events, numFound)) =>
      getEvents(List(eventId)) pipeTo self
      stay()

    case Event(FoundEvents(foundEvents), ProcessingRequest(client, query, events, numFound)) =>
      if (numFound + foundEvents.length < limit)
        query.nextPosting pipeTo self
      stay() using ProcessingRequest(client, query, events ++ foundEvents, numFound + foundEvents.length)

    case Event(GetEvents, ProcessingRequest(client, query, events, numFound)) =>
      stay()
  }

  when(SendingRequest) {
    case Event(SendEvents, SendingRequest(client, events)) =>
      client ! events
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), events.length)
  }

  when(FinishedQuery) {
    case Event(DeleteQuery, finishedQuery: FinishedQuery) =>
      context.stop(self)
      stay()
  }

  initialize()

  /**
   * Recursively descend the Matchers tree replacing TermMatchers with Terms.
   *
   * @param matchers
   * @return
   */
  def buildTerms(matchers: Matchers, fields: FieldsChanged, keyspace: Keyspace): Option[Matchers] = {
    matchers match {
      case TermMatcher(fieldId @ FieldIdentifier(name, EventValueType.TEXT), term: String) =>
        fields.fieldsByIdent.get(fieldId) match {
          case Some(field) =>
            Some(new Term[String](fieldId, term, keyspace, field))
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
   * @param eventIds
   * @return
   */
  def getEvents(eventIds: List[UUID]) = Future[FoundEvents] {
    if (!eventIds.isEmpty) {
      val result = keyspace.prepareQuery(CassandraSink.CF_EVENTS).getKeySlice(eventIds).execute()
      FoundEvents(result.getResult.filter(!_.getColumns.isEmpty).map(row => readEvent(row.getKey, row.getColumns)).toList)
    } else FoundEvents(List.empty)
  }

  /**
   * Parse a cassandra row and return an Event.
   *
   * @param id
   * @param columnList
   * @return
   */
  def readEvent(id: UUID, columnList: ColumnList[String]): BierEvent = {
    val event = new BierEvent(id)
    columnList foreach { column =>
      fields.fieldsByCf.get(column.getName) match {
        case Some(field: Field) =>
          field.fieldId match {
            case FieldIdentifier(fieldName, EventValueType.TEXT) =>
              event.set(fieldName, column.getStringValue)
            case FieldIdentifier(fieldName, EventValueType.LITERAL) =>
              val literal: List[String] = column.getValue(CassandraSink.SER_LITERAL).toList
              event.set(fieldName, literal)
            case FieldIdentifier(fieldName, EventValueType.INTEGER) =>
              event.set(fieldName, column.getLongValue)
            case FieldIdentifier(fieldName, EventValueType.FLOAT) =>
              event.set(fieldName, column.getDoubleValue)
            case FieldIdentifier(fieldName, EventValueType.DATETIME) =>
              event.set(fieldName, new DateTime(column.getDateValue.getTime, DateTimeZone.UTC))
            case FieldIdentifier(fieldName, EventValueType.ADDRESS) =>
              event.set(fieldName, InetAddress.getByAddress(column.getByteArrayValue))
            case FieldIdentifier(fieldName, EventValueType.HOSTNAME) =>
              event.set(field.fieldId.fieldName, Name.fromString(column.getStringValue))
            }
        case None =>
          log.error("failed to read column {} from event {}; no such field", column.getName, id)
      }
    }
    event
  }
}

object Query {
  /* query case classes */
  case object GetEvents
  case class FoundEvents(events: List[BierEvent])
  case object SendEvents
  case object DescribeQuery
  case object DeleteQuery
  case class QueryStatistics(id: UUID, created: DateTime, state: String)


  /* our FSM states */
  sealed trait State
  case object WaitingForMatcherEstimates extends State
  case object WaitingForRequest extends State
  case object ProcessingRequest extends State
  case object SendingRequest extends State
  case object FinishedQuery extends State

  sealed trait Data
  case object EmptyQuery extends Data
  case class WaitingForRequest(matchers: Matchers, events: List[BierEvent], numFound: Int) extends Data
  case class ProcessingRequest(client: ActorRef, matchers: Matchers, events: List[BierEvent], numFound: Int) extends Data
  case class SendingRequest(client: ActorRef, events: List[BierEvent]) extends Data
  case class FinishedRequest(events: List[BierEvent]) extends Data
  case class FinishedQuery(finished: DateTime, numSent: Int) extends Data
}
