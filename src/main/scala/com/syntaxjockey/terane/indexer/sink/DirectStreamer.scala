package com.syntaxjockey.terane.indexer.sink

import akka.actor.LoggingFSM
import scala.math.min
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent}
import com.syntaxjockey.terane.indexer.sink.DirectStreamer.{State, Data}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store

class DirectStreamer(id: UUID, createQuery: CreateQuery, store: Store) extends LoggingFSM[State,Data] {
  import DirectStreamer._
  import Query._

  val config = context.system.settings.config

  val defaultBatchSize = config.getInt("terane.queries.default-batch-size")
  val maxBatchSize = config.getInt("terane.queries.maximum-batch-size")

  startWith(ReceivingEvents, ReceivingEvents(List.empty, 0, 0, 0))

  when(ReceivingEvents) {

    case Event(event: BierEvent, ReceivingEvents(events, sequence, numRead, numSent)) =>
      if (createQuery.limit.isDefined && createQuery.limit.get == numRead + 1)
        goto(ReceivedEvents) using ReceivedEvents(events :+ event, sequence, numRead + 1, numSent)
      else
        stay() using ReceivingEvents(events :+ event, sequence, numRead + 1, numSent) replying NextEvent

    case Event(NoMoreEvents, ReceivingEvents(events, sequence, numRead, numSent)) =>
      goto(ReceivedEvents) using ReceivedEvents(events, sequence, numRead, numSent)

    case Event(GetEvents(_, Some(limit)), ReceivingEvents(events, sequence, numRead, numSent)) =>
      val _limit = getBatchSize(numSent, limit)
      val toSend = events.take(_limit)
      val batch = EventSet(toSend, finished = false)
      stay() using ReceivingEvents(events.drop(_limit), sequence + 1, numRead, numSent + toSend.length) replying batch

    case Event(GetEvents(_, None), ReceivingEvents(events, sequence, numRead, numSent)) =>
      val _limit = getBatchSize(numSent, defaultBatchSize)
      val toSend = events.take(_limit)
      val batch = EventSet(toSend, finished = false)
      stay() using ReceivingEvents(events.drop(_limit), sequence + 1, numRead, numSent + toSend.length) replying batch

    case Event(QueryStatistics(_, created, _, _, _), receivingEvents: ReceivingEvents) =>
      stay() replying QueryStatistics(id, created, "receiving events", receivingEvents.numRead, receivingEvents.numSent)
  }

  onTransition {
    case ReceivingEvents -> ReceivedEvents => context.parent ! FinishedReading
  }

  when(ReceivedEvents) {

    case Event(GetEvents(_, _), ReceivedEvents(events, sequence, numRead, numSent)) if events.isEmpty =>
      stay() using ReceivedEvents(events, sequence + 1, numRead, numSent) replying EventSet(events, finished = true)

    case Event(GetEvents(_, Some(limit)), ReceivedEvents(events, sequence, numRead, numSent)) =>
      val _limit = getBatchSize(numSent, limit)
      val toSend = events.take(_limit)
      val eventsLeft = events.drop(_limit)
      val batch = EventSet(toSend, if (eventsLeft.isEmpty) true else false)
      stay() using ReceivedEvents(eventsLeft, sequence + 1, numRead, numSent + toSend.length) replying batch

    case Event(GetEvents(_, None), ReceivedEvents(events, sequence, numRead, numSent)) =>
      val _limit = getBatchSize(numSent, defaultBatchSize)
      val toSend = events.take(_limit)
      val eventsLeft = events.drop(_limit)
      val batch = EventSet(toSend, if (eventsLeft.isEmpty) true else false)
      stay() using ReceivedEvents(eventsLeft, sequence + 1, numRead, numSent + toSend.length) replying batch

    case Event(QueryStatistics(_, created, _, _, _), receivedEvents: ReceivedEvents) =>
      stay() replying QueryStatistics(id, created, "received events", receivedEvents.numRead, receivedEvents.numSent)
  }

  initialize()

  def getBatchSize(numSent: Int, requestedSize: Int): Int = {
    createQuery.limit match {
      case Some(queryLimit) =>
        min(if (requestedSize > maxBatchSize) maxBatchSize else requestedSize, queryLimit - numSent)
      case None =>
        if (requestedSize > maxBatchSize) maxBatchSize else requestedSize
    }
  }

}

object DirectStreamer {

  sealed trait State
  case object ReceivingEvents extends State
  case object ReceivedEvents extends State

  sealed trait Data
  case class ReceivingEvents(events: List[BierEvent], sequence: Int, numRead: Int, numSent: Int) extends Data
  case class ReceivedEvents(events: List[BierEvent], sequence: Int, numRead: Int, numSent: Int) extends Data
}
