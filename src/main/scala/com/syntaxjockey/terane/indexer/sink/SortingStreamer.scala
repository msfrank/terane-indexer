package com.syntaxjockey.terane.indexer.sink

import akka.actor.{ActorRef, LoggingFSM}
import org.mapdb.{DBMaker, Serializer, BTreeKeySerializer}
import org.joda.time.{DateTimeZone, DateTime}
import org.xbill.DNS.Name
import scala.math.min
import scala.collection.mutable
import java.io.{DataInput, DataOutput, File}
import java.net.InetAddress
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{Event => BierEvent, EventValueType}
import com.syntaxjockey.terane.indexer.sink.SortingStreamer.{State, Data}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldsChanged
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier

class SortingStreamer(id: UUID, createQuery: CreateQuery, fields: FieldsChanged) extends LoggingFSM[State,Data] {
  import SortingStreamer._
  import Query._
  import BierEvent._

  val config = context.system.settings.config
  val defaultBatchSize = config.getInt("terane.queries.default-batch-size")
  val maxBatchSize = config.getInt("terane.queries.maximum-batch-size")

  // create the sorting table
  val sortFields: Array[FieldIdentifier] = createQuery.sortBy.get.toArray
  val ident2index: Map[FieldIdentifier,Int] = fields.fieldsByIdent.keys.zip(0 to fields.fieldsByIdent.size).toMap
  val keySerializer = new EventKeySerializer(sortFields)
  val valueSerializer = new EventSerializer(ident2index)
  val dbfile = new File("sorted-" + id)
  val db = DBMaker.newFileDB(dbfile)
            .compressionEnable()
            .writeAheadLogDisable()
            .make()
  val sortedEvents = db.createTreeMap("events", 16, true, false, keySerializer, valueSerializer, null)
  log.debug("created sort table " + dbfile.getAbsolutePath)

  startWith(ReceivingEvents, ReceivingEvents(0, List.empty))

  when(ReceivingEvents) {

    case Event(event: BierEvent, ReceivingEvents(numRead, deferredRequests)) =>
      sortedEvents.put(makeKey(event), event)
      stay() using ReceivingEvents(numRead + 1, deferredRequests) replying NextEvent

    case Event(NoMoreEvents, ReceivingEvents(numRead, deferredRequests)) =>
      goto(ReceivedEvents) using ReceivedEvents(0, sortedEvents.entrySet().iterator(), numRead, 0)

    case Event(GetEvents(_), ReceivingEvents(numRead, deferredRequests)) =>
      stay() using ReceivingEvents(numRead, deferredRequests :+ sender)

    case Event(QueryStatistics(_, created, _, _, _), receivingEvents: ReceivingEvents) =>
      stay() replying QueryStatistics(id, created, "sorting events", receivingEvents.numRead, 0)
  }

  onTransition {
    case ReceivingEvents -> ReceivedEvents => context.parent ! FinishedReading
  }

  when(ReceivedEvents) {

    case Event(GetEvents(_), ReceivedEvents(sequence, iterator, numRead, numSent)) if createQuery.limit == Some(numSent) =>
      stay() using ReceivedEvents(sequence + 1, iterator, numRead, numSent) replying EventsBatch(sequence, List.empty, finished = true)

    case Event(GetEvents(_), ReceivedEvents(sequence, iterator, numRead, numSent)) if !iterator.hasNext =>
      stay() using ReceivedEvents(sequence + 1, iterator, numRead, numSent) replying EventsBatch(sequence, List.empty, finished = true)

    case Event(GetEvents(Some(limit)), ReceivedEvents(sequence, iterator, numRead, numSent)) =>
      val _limit = getBatchSize(numSent, limit)
      val toSend = new mutable.MutableList[BierEvent]
      for (i <- 0.until(_limit) if iterator.hasNext) { toSend += iterator.next().getValue }
      val batch = EventsBatch(sequence, toSend.toList, if (iterator.hasNext) false else true)
      stay() using ReceivedEvents(sequence + 1, iterator, numRead, numSent + toSend.length) replying batch

    case Event(GetEvents(None), ReceivedEvents(sequence, iterator, numRead, numSent)) =>
      val _limit = getBatchSize(numSent, defaultBatchSize)
      val toSend = new mutable.MutableList[BierEvent]
      for (i <- 0.until(_limit) if iterator.hasNext) { toSend += iterator.next().getValue }
      val batch = EventsBatch(sequence, toSend.toList, if (iterator.hasNext) false else true)
      stay() using ReceivedEvents(sequence + 1, iterator, numRead, numSent + toSend.length) replying batch

    case Event(QueryStatistics(_, created, _, _, _), receivedEvents: ReceivedEvents) =>
      stay() replying QueryStatistics(id, created, "received events", receivedEvents.numRead, receivedEvents.numSent)
  }

  initialize()

  onTermination {
    case StopEvent(_, _, _) =>
      sortedEvents.close()
      dbfile.delete()
      log.debug("deleted sort file " + dbfile.getAbsolutePath)
  }

  def makeKey(event: BierEvent): EventKey = {
    val keyvalues: Array[Option[BierEvent.KeyValue]] = sortFields.map { ident: FieldIdentifier =>
      if (event.values.contains(ident)) Some(ident -> event.values(ident)) else Some(ident -> Value())
    }
    EventKey(keyvalues)
  }

  def getBatchSize(numSent: Int, requestedSize: Int): Int = {
    createQuery.limit match {
      case Some(queryLimit) =>
        min(if (requestedSize > maxBatchSize) maxBatchSize else requestedSize, queryLimit - numSent)
      case None =>
        if (requestedSize > maxBatchSize) maxBatchSize else requestedSize
    }
  }

}

object SortingStreamer {

  sealed trait State
  case object ReceivingEvents extends State
  case object ReceivedEvents extends State

  sealed trait Data
  case class ReceivingEvents(numRead: Int, deferredRequests: List[ActorRef]) extends Data
  case class ReceivedEvents(sequence: Int, entries: java.util.Iterator[java.util.Map.Entry[EventKey,BierEvent]], numRead: Int, numSent: Int) extends Data
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
      case (Some((ident1: FieldIdentifier, v1: Value)), Some((ident2: FieldIdentifier, v2: Value))) =>
        if (ident1 != ident2)
          throw new Exception("can't compare %s and %s".format(ident1, ident2))
        val comparison: Int = ident1.fieldType match {
          case EventValueType.TEXT =>
            if (!v1.text.isDefined && v2.text.isDefined) return -1
            if (v1.text.isDefined && !v2.text.isDefined) return 1
            if (v1.text.isDefined && v2.text.isDefined) v1.text.get.compareTo(v2.text.get) else 0
          case EventValueType.LITERAL =>
            throw new Exception("don't know how to sort " + ident1)
          case EventValueType.INTEGER =>
            if (!v1.integer.isDefined && v2.integer.isDefined) return -1
            if (v1.integer.isDefined && !v2.integer.isDefined) return 1
            if (v1.integer.isDefined && v2.integer.isDefined) v1.integer.get.compareTo(v2.integer.get) else 0
          case EventValueType.FLOAT =>
            if (!v1.float.isDefined && v2.float.isDefined) return -1
            if (v1.float.isDefined && !v2.float.isDefined) return 1
            if (v1.float.isDefined && v2.float.isDefined) v1.float.get.compareTo(v2.float.get) else 0
          case EventValueType.DATETIME =>
            if (!v1.datetime.isDefined && v2.datetime.isDefined) return -1
            if (v1.datetime.isDefined && !v2.datetime.isDefined) return 1
            if (v1.datetime.isDefined && v2.datetime.isDefined) v1.datetime.get.compareTo(v2.datetime.get) else 0
          case EventValueType.ADDRESS =>
//            if (!v1.address.isDefined && v2.address.isDefined) return -1
//            if (v1.address.isDefined && !v2.address.isDefined) return 1
//            if (v1.address.isDefined && v2.address.isDefined) v1.address.get.compareTo(v2.address.get) else 0
            throw new Exception("don't know how to sort " + ident1)
          case EventValueType.HOSTNAME =>
            if (!v1.hostname.isDefined && v2.hostname.isDefined) return -1
            if (v1.hostname.isDefined && !v2.hostname.isDefined) return 1
            if (v1.hostname.isDefined && v2.hostname.isDefined) v1.hostname.get.compareTo(v2.hostname.get) else 0
        }
        if (comparison != 0) return comparison
    }
    throw new Exception("fell off the end of the comparison")
  }
}

// FIXME: implement delta-encoding https://en.wikipedia.org/wiki/Delta_encoding

class EventKeySerializer(sortFields: Array[FieldIdentifier]) extends BTreeKeySerializer[EventKey] with java.io.Serializable {
  import BierEvent._

  override def serialize(out: DataOutput, start: Int, end: Int, keys: Array[Object]) {
    (start to end).foreach { index => serializeEventKey(out, keys(index).asInstanceOf[EventKey]) }
  }

  def serializeEventKey(out: DataOutput, eventKey: EventKey) {
    var keysTaken = eventKey.keyvalues.take(8)
    var keysLeft = eventKey.keyvalues.drop(8)
    while (keysTaken.length > 0) {
      var bitmap: Int = 0
      0 to 7 foreach { i => if (keysTaken(i).isDefined) bitmap = bitmap | (1 << i) }
      out.writeByte(bitmap)
      eventKey.keyvalues.foreach {
        case Some((ident: FieldIdentifier, value: BierEvent.Value)) =>
          ident.fieldType match {
            case EventValueType.TEXT =>
              out.writeUTF(value.text.get)
            case EventValueType.LITERAL =>
              throw new Exception("don't know how to sort " + ident)
            case EventValueType.INTEGER =>
              out.writeLong(value.integer.get)
            case EventValueType.FLOAT =>
              out.writeDouble(value.float.get)
            case EventValueType.DATETIME =>
              out.writeLong(value.datetime.get.getMillis)
            case EventValueType.ADDRESS =>
              val address = value.address.get
              val addrBytes = address.getAddress
              out.writeInt(addrBytes.length)
              out.write(address.getAddress)
            case EventValueType.HOSTNAME =>
              out.writeUTF(value.hostname.get.toString)
          }
        case None =>
      }
      keysTaken = keysLeft.take(8)
      keysLeft = keysLeft.drop(8)
    }
  }

  override def deserialize(in: DataInput, start: Int, end: Int, size: Int): Array[Object] = {
    (start to end).map(_ => deserializeEventKey(in)).toArray
  }

  def deserializeEventKey(in: DataInput): EventKey = {
    var bitmap: Int = 0
    var i = 0
    val keyvalues: Array[Option[BierEvent.KeyValue]] = sortFields.map { ident: FieldIdentifier =>
      if (i == 8)
        bitmap = in.readByte()
      if ((bitmap & (1 << i)) > 0) {
        ident.fieldType match {
          case EventValueType.TEXT =>
            Some(ident -> Value(text = Some(in.readUTF())))
          case EventValueType.LITERAL =>
            throw new Exception("don't know how to sort " + ident)
          case EventValueType.INTEGER =>
            Some(ident -> Value(integer = Some(in.readLong())))
          case EventValueType.FLOAT =>
            Some(ident -> Value(float = Some(in.readDouble())))
          case EventValueType.DATETIME =>
            Some(ident -> Value(datetime = Some(new DateTime(in.readLong(), DateTimeZone.UTC))))
          case EventValueType.ADDRESS =>
            val length = in.readInt()
            val bytes = new Array[Byte](length)
            in.readFully(bytes)
            Some(ident -> Value(address = Some(InetAddress.getByAddress(bytes))))
          case EventValueType.HOSTNAME =>
            Some(ident -> Value(hostname = Some(Name.fromString(in.readUTF()))))
        }
      } else Some(ident -> Value())
    }
    EventKey(keyvalues)
  }
}

class EventSerializer(lookup: Map[FieldIdentifier,Int]) extends Serializer[BierEvent] with java.io.Serializable {
  import BierEvent._

  val reverse: Map[Int,FieldIdentifier] = lookup.map(x => x._2 -> x._1)

  override def serialize(out: DataOutput, event: BierEvent) {
    // write id
    out.writeLong(event.id.getMostSignificantBits)
    out.writeLong(event.id.getLeastSignificantBits)
    // write each field
    event.values.foreach { case (ident: FieldIdentifier, value: Value) =>
      out.writeInt(lookup(ident))
      ident.fieldType match {
        case EventValueType.TEXT =>
          out.writeUTF(value.text.get)
        case EventValueType.LITERAL =>
          val literal = value.literal.get
          out.writeInt(literal.length)
          literal.foreach(out.writeUTF)
        case EventValueType.INTEGER =>
          out.writeLong(value.integer.get)
        case EventValueType.FLOAT =>
          out.writeDouble(value.float.get)
        case EventValueType.DATETIME =>
          out.writeLong(value.datetime.get.getMillis)
        case EventValueType.ADDRESS =>
          val address = value.address.get.getAddress
          out.writeInt(address.length)
          out.write(address)
        case EventValueType.HOSTNAME =>
          out.writeUTF(value.hostname.get.toString)
      }
    }
  }

  override def deserialize(in: DataInput, available: Int): BierEvent = {
    // read id
    val id = new UUID(in.readLong(), in.readLong())
    var keyvalues: Map[FieldIdentifier,Value] = Map.empty
    while (available > 0) {
      // read lookup key
      val ident = reverse(in.readInt())
      val keyvalue = ident.fieldType match {
        case EventValueType.TEXT =>
          ident -> Value(text = Some(in.readUTF()))
        case EventValueType.LITERAL =>
          val length = in.readInt()
          val literal: List[String] = for (_ <- 0.to(length).toList) yield in.readUTF()
          ident -> Value(literal = Some(literal))
        case EventValueType.INTEGER =>
          ident -> Value(integer = Some(in.readLong()))
        case EventValueType.FLOAT =>
          ident -> Value(float = Some(in.readDouble()))
        case EventValueType.DATETIME =>
          ident -> Value(datetime = Some(new DateTime(in.readLong(), DateTimeZone.UTC)))
        case EventValueType.ADDRESS =>
          val length = in.readInt()
          val bytes = new Array[Byte](length)
          in.readFully(bytes)
          ident -> Value(address = Some(InetAddress.getByAddress(bytes)))
        case EventValueType.HOSTNAME =>
          ident -> Value(hostname = Some(Name.fromString(in.readUTF())))
      }
      keyvalues = keyvalues + keyvalue
    }
    new BierEvent(id, keyvalues)
  }
}