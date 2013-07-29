package com.syntaxjockey.terane.indexer.sink

import akka.actor.LoggingFSM
import org.joda.time.DateTime
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent}
import com.syntaxjockey.terane.indexer.sink.Query.{QueryStatistics, NextEvent, GetEvents}
import com.syntaxjockey.terane.indexer.sink.Streamer.{State, Data}

class Streamer(id: UUID) extends LoggingFSM[State,Data] {
  import Streamer._

  val limit = 100

  startWith(ReceivingEvents, ReceivingEvents(List.empty, 0))

  when(ReceivingEvents) {

    case Event(event: BierEvent, ReceivingEvents(events, sequence)) =>
      stay() using ReceivingEvents(events :+ event, sequence) replying NextEvent

    case Event(NoMoreEvents, ReceivingEvents(events, sequence)) =>
      goto(ReceivedEvents) using ReceivedEvents(events, sequence)

    case Event(GetEvents(Some(_limit)), ReceivingEvents(events, sequence)) =>
      stay() using ReceivingEvents(events.drop(_limit), sequence + 1) replying EventsBatch(sequence, events.take(_limit), finished = false)

    case Event(GetEvents(None), ReceivingEvents(events, sequence)) =>
      stay() using ReceivingEvents(events.drop(limit), sequence + 1) replying EventsBatch(sequence, events.take(limit), finished = false)

    case Event(QueryStatistics(_, created, _), _) =>
      stay() replying StreamerQueryStatistics(id, created, "receiving events")
  }

  onTransition {
    case ReceivingEvents -> ReceivedEvents => context.parent ! FinishedReading
  }

  when(ReceivedEvents) {

    case Event(GetEvents(_), ReceivedEvents(events, sequence)) if events.isEmpty =>
      stay() using ReceivedEvents(events, sequence + 1) replying EventsBatch(sequence, events, finished = true)

    case Event(GetEvents(Some(_limit)), ReceivedEvents(events, sequence)) =>
      val eventsLeft = events.drop(_limit)
      stay() using ReceivedEvents(eventsLeft, sequence + 1) replying EventsBatch(sequence, events.take(_limit), if (eventsLeft.isEmpty) true else false)

    case Event(GetEvents(None), ReceivedEvents(events, sequence)) =>
      val eventsLeft = events.drop(limit)
      stay() using ReceivedEvents(eventsLeft, sequence + 1) replying EventsBatch(sequence, events.take(limit), if (eventsLeft.isEmpty) true else false)

    case Event(QueryStatistics(_, created, _), _) =>
      stay() replying StreamerQueryStatistics(id, created, "received events")
  }

  initialize()
}

object Streamer {
  case object NoMoreEvents
  case object FinishedReading
  case class EventsBatch(sequence: Int, events: List[BierEvent], finished: Boolean)
  case class StreamerQueryStatistics(id: UUID, created: DateTime, state: String)

  sealed trait State
  case object ReceivingEvents extends State
  case object ReceivedEvents extends State

  sealed trait Data
  case class ReceivingEvents(events: List[BierEvent], sequence: Int) extends Data
  case class ReceivedEvents(events: List[BierEvent], sequence: Int) extends Data
}
