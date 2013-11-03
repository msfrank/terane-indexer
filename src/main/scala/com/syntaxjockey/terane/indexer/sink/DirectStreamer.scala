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

import akka.actor.{Props, ActorRef, LoggingFSM}
import org.joda.time.DateTime
import org.mapdb.{BTreeKeySerializer, DBMaker}
import scala.collection.mutable
import java.io.File
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{FieldIdentifier, BierEvent}
import com.syntaxjockey.terane.indexer.sink.DirectStreamer.{State, Data}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldMap
import com.syntaxjockey.terane.indexer.sink.Query.GetEvents
import java.nio.file.Files

class DirectStreamer(id: UUID, createQuery: CreateQuery, created: DateTime, fields: FieldMap) extends Streamer(created, fields) with LoggingFSM[State,Data] {
  import DirectStreamer._
  import Query._

  val config = context.system.settings.config

  val valueSerializer = new EventSerializer(ident2index)
  val dbfile = new File("temp-" + id)
  val db = DBMaker.newFileDB(dbfile)
    .compressionEnable()
    .transactionDisable()
    .deleteFilesAfterClose()
    .make()
  val events = db.createTreeMap("events")
    .nodeSize(16)
    .valuesStoredOutsideNodes(false)
    .keepCounter(false)
    .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
    .valueSerializer(valueSerializer)
    .make[Long,BierEvent]()
  log.debug("created temp table " + dbfile.getAbsolutePath)

  startWith(ReceivingEvents, ReceivingEvents(0, Seq.empty))

  when(ReceivingEvents) {

    case Event(event: BierEvent, ReceivingEvents(numRead, deferredGetEvents)) =>
      events.put(numRead, event)
      if (createQuery.limit.isDefined && createQuery.limit.get == numRead + 1) {
        val reverse = createQuery.reverse.getOrElse(false)
        val entries = if (reverse) events.descendingMap().entrySet() else events.entrySet()
        goto(ReceivedEvents) using ReceivedEvents(entries, numRead + 1, 0, 0)
      } else
        stay() using ReceivingEvents(numRead + 1, deferredGetEvents) replying NextEvent

    case Event(NoMoreEvents, ReceivingEvents(numRead, deferredGetEvents)) =>
      val reverse = createQuery.reverse.getOrElse(false)
      val entries = if (reverse) events.descendingMap().entrySet() else events.entrySet()
      goto(ReceivedEvents) using ReceivedEvents(entries, numRead, 0, 0)

    case Event(getEvents: GetEvents, ReceivingEvents(numRead, deferredGetEvents)) =>
      stay() using ReceivingEvents(numRead, deferredGetEvents :+ DeferredGetEvents(sender, getEvents))

    case Event(DescribeQuery, receivingEvents: ReceivingEvents) =>
      stay() replying QueryStatistics(id, created, "receiving events", receivingEvents.numRead, 0, getRuntime)
  }

  onTransition {
    case ReceivingEvents -> ReceivedEvents =>
      context.parent ! FinishedReading
      stateData match {
        case ReceivingEvents(_, deferredGetEvents) =>
          deferredGetEvents.foreach {
            case DeferredGetEvents(_sender, getEvents) => self.tell(getEvents, _sender)
          }
        case _ =>
      }
  }

  when(ReceivedEvents) {

    case Event(GetEvents(offset: Option[Int], limit: Option[Int]), ReceivedEvents(entries, numRead, numQueries, numSent)) =>
      val iterator: java.util.Iterator[java.util.Map.Entry[Long,BierEvent]] = entries.iterator()
      // FIXME: cache iterators
      // move iterator to the specified offset
      for (_offset <- offset; i <- 0.until(_offset) if iterator.hasNext) { iterator.next() }
      // build event set
      val toSend = new mutable.MutableList[BierEvent]
      if (limit.isDefined)
        for (i <- 0.until(limit.get) if iterator.hasNext) { toSend += iterator.next().getValue }
      else
        while (iterator.hasNext) { toSend += iterator.next().getValue }
      val fields: Map[String,FieldIdentifier] = index2ident.map(e => index2key(e._1) -> e._2)
      val stats = QueryStatistics(id, created, "received events", numRead, numSent, getRuntime)
      val finished = if (iterator.hasNext) false else true
      val eventSet = EventSet(fields, toSend.toList, stats, finished)
      stay() using ReceivedEvents(entries, numRead, numQueries + 1, numSent + toSend.length) replying eventSet

    case Event(DescribeQuery, receivedEvents: ReceivedEvents) =>
      stay() replying QueryStatistics(id, created, "received events", receivedEvents.numRead, receivedEvents.numSent, getRuntime)
  }

  initialize()

  onTermination {
    case StopEvent(_, _, _) =>
      db.close()
  }
}

object DirectStreamer {

  def props(id: UUID, createQuery: CreateQuery, created: DateTime, fields: FieldMap) = {
    Props(classOf[DirectStreamer], id, createQuery, created, fields)
  }

  case class DeferredGetEvents(sender: ActorRef, getEvents: GetEvents)

  sealed trait State
  case object ReceivingEvents extends State
  case object ReceivedEvents extends State

  sealed trait Data
  case class ReceivingEvents(numRead: Int, deferredGetEvents: Seq[DeferredGetEvents]) extends Data
  case class ReceivedEvents(entries: java.util.Set[java.util.Map.Entry[Long,BierEvent]], numRead: Int, numQueries: Int, numSent: Int) extends Data
}
