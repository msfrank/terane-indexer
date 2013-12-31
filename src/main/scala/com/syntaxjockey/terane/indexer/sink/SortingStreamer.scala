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
import org.mapdb.{DBMaker, Serializer, BTreeKeySerializer}
import org.joda.time.{DateTimeZone, DateTime}
import org.xbill.DNS.Name
import scala.collection.mutable
import java.io.{DataInput, DataOutput, File}
import java.net.InetAddress
import java.util.UUID

import com.syntaxjockey.terane.indexer.{CreateQuery,DescribeQuery,GetEvents,QueryStatistics,EventSet}
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.{BierEvent, EventValue}
import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.SortingStreamer.{State, Data}
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldMap

class SortingStreamer(id: UUID, createQuery: CreateQuery, created: DateTime, fields: FieldMap) extends Streamer(created, fields) with LoggingFSM[State,Data] {
  import SortingStreamer._
  import Query._

  val config = context.system.settings.config

  // create the sorting table
  val sortFields: Array[FieldIdentifier] = createQuery.sortBy.get.toArray
  val keySerializer = new EventKeySerializer(sortFields)
  val valueSerializer = new EventSerializer(ident2index)
  val dbfile = new File("sort-" + id)
  val db = DBMaker.newFileDB(dbfile)
            .compressionEnable()
            .transactionDisable()
            .deleteFilesAfterClose()
            .make()
  val events = db.createTreeMap("events")
    .nodeSize(16)
    .valuesStoredOutsideNodes(true)
    .keepCounter(false)
    .keySerializer(keySerializer)
    .valueSerializer(valueSerializer)
    .make[EventKey,BierEvent]()
  log.debug("created sort table " + dbfile.getAbsolutePath)

  startWith(ReceivingEvents, ReceivingEvents(0, 0, Seq.empty))

  when(ReceivingEvents) {

    case Event(event: BierEvent, ReceivingEvents(numRead, currentSize, deferredGetEvents)) =>
      events.put(makeKey(event), event)
      val updatedSize = if (createQuery.limit.isDefined && createQuery.limit.get == currentSize) {
        events.remove(events.lastKey())
        currentSize
      } else currentSize + 1
      stay() using ReceivingEvents(numRead + 1, updatedSize, deferredGetEvents) replying NextEvent

    case Event(NoMoreEvents, ReceivingEvents(numRead, _, _)) =>
      val reverse = createQuery.reverse.getOrElse(false)
      val entries = if (reverse) events.descendingMap().entrySet() else events.entrySet()
      goto(ReceivedEvents) using ReceivedEvents(entries, numRead, 0, 0)

    case Event(getEvents: GetEvents, ReceivingEvents(numRead, currentSize, deferredGetEvents)) =>
      stay() using ReceivingEvents(numRead, currentSize, deferredGetEvents :+ DeferredGetEvents(sender, getEvents))

    case Event(DescribeQuery, receivingEvents: ReceivingEvents) =>
      stay() replying QueryStatistics(id, created, "sorting events", receivingEvents.numRead, 0, getRuntime)
  }

  onTransition {
    case ReceivingEvents -> ReceivedEvents =>
      context.parent ! FinishedReading
      stateData match {
        case ReceivingEvents(_, _, deferredGetEvents) =>
          deferredGetEvents.foreach {
            case DeferredGetEvents(_sender, getEvents) => self.tell(getEvents, _sender)
          }
        case _ =>
      }
  }

  when(ReceivedEvents) {

    case Event(GetEvents(offset: Option[Int], limit: Option[Int]), ReceivedEvents(entries, numRead, numQueries, numSent)) =>
      val iterator: java.util.Iterator[java.util.Map.Entry[EventKey,BierEvent]] = entries.iterator()
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

  def makeKey(event: BierEvent): EventKey = {
    val keyvalues: Array[Option[BierEvent.KeyValue]] = sortFields.map { ident: FieldIdentifier =>
      if (event.values.contains(ident)) Some(ident -> event.values(ident)) else Some(ident -> EventValue())
    }
    EventKey(keyvalues)
  }
}

object SortingStreamer {

  def props(id: UUID, createQuery: CreateQuery, created: DateTime, fields: FieldMap) = {
    Props(classOf[SortingStreamer], id, createQuery, created, fields)
  }

  case class DeferredGetEvents(sender: ActorRef, getEvents: GetEvents)

  sealed trait State
  case object ReceivingEvents extends State
  case object ReceivedEvents extends State

  sealed trait Data
  case class ReceivingEvents(numRead: Int, currentSize: Int, deferredGetEvents: Seq[DeferredGetEvents]) extends Data
  case class ReceivedEvents(entries: java.util.Set[java.util.Map.Entry[EventKey,BierEvent]], numRead: Int, numQueries: Int, numSent: Int) extends Data
}

case class EventKey(keyvalues: Array[Option[BierEvent.KeyValue]]) extends Comparable[EventKey] {
  import BierEvent._

  def compareTo(other: EventKey): Int = {
    keyvalues.zipAll(other.keyvalues, None, None).foreach {
      case (None, None) =>
        return 0
      case (Some(kv1: KeyValue), None) =>
        throw new Exception("event keys are not symmetrical")
      case (None, Some(kv2: KeyValue)) =>
        throw new Exception("event keys are not symmetrical")
      case (Some((ident1: FieldIdentifier, v1: EventValue)), Some((ident2: FieldIdentifier, v2: EventValue))) =>
        if (ident1 != ident2)
          throw new Exception("can't compare %s and %s".format(ident1, ident2))
        val comparison: Int = ident1.fieldType match {
          case DataType.TEXT =>
            if (!v1.text.isDefined && v2.text.isDefined) return -1
            if (v1.text.isDefined && !v2.text.isDefined) return 1
            if (v1.text.isDefined && v2.text.isDefined) v1.text.get.compareTo(v2.text.get) else 0
          case DataType.LITERAL =>
            if (!v1.literal.isDefined && v2.literal.isDefined) return -1
            if (v1.literal.isDefined && !v2.literal.isDefined) return 1
            if (v1.literal.isDefined && v2.literal.isDefined) v1.literal.get.compareTo(v2.literal.get) else 0
          case DataType.INTEGER =>
            if (!v1.integer.isDefined && v2.integer.isDefined) return -1
            if (v1.integer.isDefined && !v2.integer.isDefined) return 1
            if (v1.integer.isDefined && v2.integer.isDefined) v1.integer.get.compareTo(v2.integer.get) else 0
          case DataType.FLOAT =>
            if (!v1.float.isDefined && v2.float.isDefined) return -1
            if (v1.float.isDefined && !v2.float.isDefined) return 1
            if (v1.float.isDefined && v2.float.isDefined) v1.float.get.compareTo(v2.float.get) else 0
          case DataType.DATETIME =>
            if (!v1.datetime.isDefined && v2.datetime.isDefined) return -1
            if (v1.datetime.isDefined && !v2.datetime.isDefined) return 1
            if (v1.datetime.isDefined && v2.datetime.isDefined) v1.datetime.get.compareTo(v2.datetime.get) else 0
          case DataType.ADDRESS =>
            if (!v1.address.isDefined && v2.address.isDefined) return -1
            if (v1.address.isDefined && !v2.address.isDefined) return 1
            if (v1.address.isDefined && v2.address.isDefined) v1.address.get.compareTo(v2.address.get) else 0
          case DataType.HOSTNAME =>
            if (!v1.hostname.isDefined && v2.hostname.isDefined) return -1
            if (v1.hostname.isDefined && !v2.hostname.isDefined) return 1
            if (v1.hostname.isDefined && v2.hostname.isDefined) v1.hostname.get.compareTo(v2.hostname.get) else 0
        }
        if (comparison != 0) return comparison
    }
    0
  }
}

// FIXME: implement delta-encoding https://en.wikipedia.org/wiki/Delta_encoding

class EventKeySerializer(sortFields: Array[FieldIdentifier]) extends BTreeKeySerializer[EventKey] with java.io.Serializable {

  override def serialize(out: DataOutput, start: Int, end: Int, keys: Array[Object]) {
    (start until end).foreach { index => serializeEventKey(out, keys(index).asInstanceOf[EventKey]) }
  }

  def serializeEventKey(out: DataOutput, eventKey: EventKey) {
    var keysTaken = eventKey.keyvalues.take(8)
    var keysLeft = eventKey.keyvalues.drop(8)
    while (keysTaken.length > 0) {
      var bitmap: Int = 0
      0 until keysTaken.length foreach { i => if (keysTaken(i) != EventValue()) bitmap = bitmap | (1 << i) }
      out.writeByte(bitmap)
      eventKey.keyvalues.foreach {
        case Some((ident: FieldIdentifier, value: EventValue)) =>
          if (value != EventValue()) ident.fieldType match {
            case DataType.TEXT =>
              out.writeUTF(value.text.get.underlying)
            case DataType.LITERAL =>
              out.writeUTF(value.literal.get.underlying)
            case DataType.INTEGER =>
              out.writeLong(value.integer.get.underlying)
            case DataType.FLOAT =>
              out.writeDouble(value.float.get.underlying)
            case DataType.DATETIME =>
              out.writeLong(value.datetime.get.underlying.getMillis)
            case DataType.ADDRESS =>
              val address = value.address.get.underlying
              val addrBytes = address.getAddress
              out.writeInt(addrBytes.length)
              out.write(address.getAddress)
            case DataType.HOSTNAME =>
              out.writeUTF(value.hostname.get.toString)
          }
        case _ =>
          throw new Exception("unexpected empty keyvalue")
      }
      keysTaken = keysLeft.take(8)
      keysLeft = keysLeft.drop(8)
    }
  }

  override def deserialize(in: DataInput, start: Int, end: Int, size: Int): Array[Object] = {
    (start until end).map(_ => deserializeEventKey(in)).toArray
  }

  def deserializeEventKey(in: DataInput): EventKey = {
    var bitmap: Int = 0
    var i = 0
    val keyvalues: Array[Option[BierEvent.KeyValue]] = sortFields.map { ident: FieldIdentifier =>
      if (i == 8)
        bitmap = in.readByte()
      if ((bitmap & (1 << i)) > 0) {
        ident.fieldType match {
          case DataType.TEXT =>
            Some(ident -> EventValue(text = Some(Text(in.readUTF()))))
          case DataType.LITERAL =>
            Some(ident -> EventValue(literal = Some(Literal(in.readUTF()))))
          case DataType.INTEGER =>
            Some(ident -> EventValue(integer = Some(Integer(in.readLong()))))
          case DataType.FLOAT =>
            Some(ident -> EventValue(float = Some(Float(in.readDouble()))))
          case DataType.DATETIME =>
            Some(ident -> EventValue(datetime = Some(Datetime(new DateTime(in.readLong(), DateTimeZone.UTC)))))
          case DataType.ADDRESS =>
            val length = in.readInt()
            val bytes = new Array[Byte](length)
            in.readFully(bytes)
            Some(ident -> EventValue(address = Some(Address(InetAddress.getByAddress(bytes)))))
          case DataType.HOSTNAME =>
            Some(ident -> EventValue(hostname = Some(Hostname(Name.fromString(in.readUTF())))))
        }
      } else Some(ident -> EventValue())
    }
    EventKey(keyvalues)
  }
}

class EventSerializer(lookup: Map[FieldIdentifier,Int]) extends Serializer[BierEvent] with java.io.Serializable {

  val reverse: Map[Int,FieldIdentifier] = lookup.map(x => x._2 -> x._1)

  override def serialize(out: DataOutput, event: BierEvent) {
    // write id
    out.writeLong(event.id.getMostSignificantBits)
    out.writeLong(event.id.getLeastSignificantBits)
    // write each field
    event.values.foreach { case (ident: FieldIdentifier, value: EventValue) =>
      out.writeInt(lookup(ident))
      ident.fieldType match {
        case DataType.TEXT =>
          out.writeUTF(value.text.get.underlying)
        case DataType.LITERAL =>
          out.writeUTF(value.literal.get.underlying)
        case DataType.INTEGER =>
          out.writeLong(value.integer.get.underlying)
        case DataType.FLOAT =>
          out.writeDouble(value.float.get.underlying)
        case DataType.DATETIME =>
          out.writeLong(value.datetime.get.underlying.getMillis)
        case DataType.ADDRESS =>
          val address = value.address.get.underlying.getAddress
          out.writeInt(address.length)
          out.write(address)
        case DataType.HOSTNAME =>
          out.writeUTF(value.hostname.get.toString)
      }
    }
  }

  override def deserialize(in: DataInput, available: Int): BierEvent = {
    // read id
    val id = new UUID(in.readLong(), in.readLong())
    var keyvalues: Map[FieldIdentifier,EventValue] = Map.empty
    while (available > 0) {
      // read lookup key
      val ident = reverse(in.readInt())
      val keyvalue = ident.fieldType match {
        case DataType.TEXT =>
          ident -> EventValue(text = Some(Text(in.readUTF())))
        case DataType.LITERAL =>
          ident -> EventValue(literal = Some(Literal(in.readUTF())))
        case DataType.INTEGER =>
          ident -> EventValue(integer = Some(Integer(in.readLong())))
        case DataType.FLOAT =>
          ident -> EventValue(float = Some(Float(in.readDouble())))
        case DataType.DATETIME =>
          ident -> EventValue(datetime = Some(Datetime(new DateTime(in.readLong(), DateTimeZone.UTC))))
        case DataType.ADDRESS =>
          val length = in.readInt()
          val bytes = new Array[Byte](length)
          in.readFully(bytes)
          ident -> EventValue(address = Some(Address(InetAddress.getByAddress(bytes))))
        case DataType.HOSTNAME =>
          ident -> EventValue(hostname = Some(Hostname(Name.fromString(in.readUTF()))))
      }
      keyvalues = keyvalues + keyvalue
    }
    new BierEvent(id, keyvalues, Set.empty)
  }
}