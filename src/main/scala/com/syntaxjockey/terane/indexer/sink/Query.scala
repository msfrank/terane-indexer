package com.syntaxjockey.terane.indexer.sink

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.Some
import scala.collection.JavaConversions._
import akka.actor._
import akka.pattern.pipe
import com.netflix.astyanax.Keyspace
import org.joda.time.{DateTimeZone, DateTime}
import com.netflix.astyanax.model.ColumnList
import org.xbill.DNS.Name
import java.util.{Date, UUID}
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
  val reapingInterval = 30.seconds
  val maybeMatchers = TickleParser.buildMatchers(createQuery.query)

  /* get term estimates and possibly reorder the query */
  maybeMatchers match {
    case Some(matchers) =>
      buildTerms(matchers, fields, keyspace) match {
        case Some(query) =>
          log.debug("parsed query '{}' => {}", createQuery.query, query)
          startWith(WaitingForRequest, WaitingForRequest(query, List.empty, 0))
          query.nextPosting pipeTo self
        case None =>
          startWith(FinishedQuery, EmptyQuery)
          scheduleDelete()
      }
    case None =>
      startWith(FinishedQuery, EmptyQuery)
      scheduleDelete()
  }

  when(WaitingForRequest) {

    case Event(DescribeQuery, _) =>
      sender ! QueryStatistics(id, created, "Waiting for client request")
      stay()

    case Event(Left(NoMoreMatches), WaitingForRequest(query, events, numFound)) =>
      query.close()
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

    case Event(DeleteQuery, WaitingForRequest(query, _, _)) =>
      query.close()
      self forward DeleteQuery
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), 0)

    case Event(DeleteQuery, FinishedRequest(_)) =>
      self forward DeleteQuery
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), 0)
  }

  when(ProcessingRequest) {

    case Event(DescribeQuery, _) =>
      sender ! QueryStatistics(id, created, "Processing client request")
      stay()

    case Event(Left(NoMoreMatches), ProcessingRequest(client, query, events, numFound)) =>
      self ! SendEvents
      query.close()
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

    case Event(DeleteQuery, ProcessingRequest(_, query, _, _)) =>
      query.close()
      self forward DeleteQuery
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), 0)

  }

  when(SendingRequest) {

    case Event(DescribeQuery, _) =>
      sender ! QueryStatistics(id, created, "Sending request")
      stay()

    case Event(SendEvents, SendingRequest(client, events)) =>
      client ! events
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), events.length)

    case Event(DeleteQuery, _) =>
      self forward DeleteQuery
      goto(FinishedQuery) using FinishedQuery(DateTime.now(DateTimeZone.UTC), 0)
  }

  when(FinishedQuery) {

    case Event(DescribeQuery, _) =>
      sender ! QueryStatistics(id, created, "Finished query")
      stay()

    case Event(DeleteQuery, finishedQuery: FinishedQuery) =>
      scheduleDelete()
      stay()
  }

  initialize()

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
              case TermMatcher(FieldIdentifier(_, EventValueType.TEXT), text: String) =>
                Some(new Term[String](fieldId, text, keyspace, field))
              case TermMatcher(FieldIdentifier(_, EventValueType.LITERAL), literal: String) =>
                Some(new Term[String](fieldId, literal, keyspace, field))
              case TermMatcher(FieldIdentifier(_, EventValueType.INTEGER), integer: Long) =>
                Some(new Term[Long](fieldId, integer, keyspace, field))
              case TermMatcher(FieldIdentifier(_, EventValueType.FLOAT), float: Double) =>
                Some(new Term[Double](fieldId, float, keyspace, field))
              case TermMatcher(FieldIdentifier(_, EventValueType.DATETIME), datetime: Date) =>
                Some(new Term[Date](fieldId, datetime, keyspace, field))
              case TermMatcher(FieldIdentifier(_, EventValueType.ADDRESS), address: Array[Byte]) =>
                Some(new Term[Array[Byte]](fieldId, address, keyspace, field))
              case TermMatcher(FieldIdentifier(_, EventValueType.HOSTNAME), hostname: String) =>
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

  /**
   * Schedule the query to be destroyed at some point in the future
   */
  def scheduleDelete() {
    context.system.scheduler.scheduleOnce(reapingInterval, self, PoisonPill)
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
