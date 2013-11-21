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

package com.syntaxjockey.terane.indexer.source

import akka.actor.ActorContext
import akka.io.{SymmetricPipePair, PipelineContext, SymmetricPipelineStage}
import akka.io.TcpPipelineHandler.WithinActorContext
import akka.event.LoggingAdapter
import akka.util.{ByteIterator, ByteString}
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.tz.CachedDateTimeZone
import org.xbill.DNS.Name
import java.nio.charset.Charset

import com.syntaxjockey.terane.indexer.bier.{BierEvent, FieldIdentifier}
import com.netflix.astyanax.util.TimeUUIDUtils
import com.syntaxjockey.terane.indexer.bier.datatypes._

/**
 * Context when performing pipeline processing.
 */
class SyslogContext(logging: LoggingAdapter, context: ActorContext, val maxMessageSize: Option[Long] = None) extends PipelineContext with WithinActorContext {
  var leftover = ByteString.empty
  var schema = Map.empty[String,FieldIdentifier]
  def getLogger = logging
  def getContext = context
}

/**
 * Contains methods common to all processing stages.
 */
trait SyslogProcessingOps {

  def getUntil(iterator: ByteIterator, f: (Byte) => Boolean): ByteIterator = {
      val child = iterator.clone()
      var n = 0
      while (!f(iterator.head)) {
        iterator.getByte
        n += 1
      }
      child.take(n)
    }

    def getUntil(iterator: ByteIterator, ch: Char): ByteIterator = {
      getUntil(iterator, b => b.toChar == ch)
    }

    def makeAsciiString(iterator: ByteIterator): String = {
      new String(iterator.toArray, SyslogProcessingOps.US_ASCII_CHARSET)
    }

    def makeUtf8String(iterator: ByteIterator): String = {
      new String(iterator.toArray, SyslogProcessingOps.UTF_8_CHARSET)
    }
}

object SyslogProcessingOps {
  val US_ASCII_CHARSET = Charset.forName("US-ASCII")
  val UTF_8_CHARSET = Charset.forName("UTF-8")
}

trait SyslogProcessingEvent
case class SyslogEvent(event: BierEvent) extends SyslogProcessingEvent
case class SyslogFrame(frame: ByteString) extends SyslogProcessingEvent
case object SyslogProcessingIncomplete extends SyslogProcessingEvent
class SyslogProcessingFailure(cause: Throwable) extends Exception(cause) with SyslogProcessingEvent

/**
 * Consume a ByteString containing zero or more framed syslog messages, and emit a
 * SyslogFrames object containing the frames and any leftover unprocessed data.
 */
class ProcessTcp extends SymmetricPipelineStage[SyslogContext, SyslogProcessingEvent, ByteString] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogProcessingEvent, ByteString] {

    /* we don't process commands */
    override def commandPipeline = {frames => throw new IllegalArgumentException() }

    /* return frames from a stream of bytes */
    override def eventPipeline = { data: ByteString =>
      val frames = processFrames(data)
      if (frames.length > 1)
        frames.map(Left(_))
      else if (frames.length == 1)
        ctx.singleEvent(frames(0))
      else
        Seq.empty
    }

    /* return the framed messages or SyslogIncomplete */
    def processFrames(data: ByteString): Seq[SyslogProcessingEvent] = {
      ctx.leftover = ctx.leftover ++ data
      var events = Seq.empty[SyslogProcessingEvent]
      var finished = false
      while (!ctx.leftover.isEmpty && !finished) {
        try {
          val iterator = ctx.leftover.iterator
          val msglenString = makeAsciiString(getUntil(iterator, ' '))
          // process the msglen
          if (!isNonzeroDigit(msglenString(0).toByte))
            throw new IllegalArgumentException("MSG-LEN must start with a non-zero digit")
          msglenString.tail.foreach { char =>
            if (!isDigit(char.toByte))
              throw new IllegalArgumentException("MSG-LEN must consist only of digits")
          }
          iterator.next()
          val msglen = msglenString.toInt
          for (maxMessageSize <- ctx.maxMessageSize if msglen > maxMessageSize)
            throw new IllegalArgumentException("MSG-LEN too large")
          val bytesleft = iterator.toByteString
          // process the frame
          if (bytesleft.length < msglen) {
            finished = true
            events = events ++ Seq(SyslogProcessingIncomplete)
          } else {
            ctx.leftover = bytesleft.drop(msglen)
            events = events ++ Seq(SyslogFrame(bytesleft.take(msglen)))
          }
        } catch {
          // if the iterator has no more data
          case ex: NoSuchElementException =>
            finished = true
            events = events ++ Seq(SyslogProcessingIncomplete)
          case ex: Throwable =>
            finished = true
            events = events ++ Seq(new SyslogProcessingFailure(ex))
        }
      }
      events
    }

    def isDigit(byte: Byte): Boolean = byte >= 0x30 && byte <= 0x39

    def isNonzeroDigit(byte: Byte): Boolean = byte >= 0x31 && byte <= 0x39
  }
}

/**
 * Consume a ByteString containing one framed syslog message, and emit a SyslogFrames
 * object containing the frame.
 */
class ProcessUdp extends SymmetricPipelineStage[SyslogContext, SyslogProcessingEvent, ByteString] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogProcessingEvent, ByteString] {

    /* we don't process commands */
    override def commandPipeline = {frames => throw new IllegalArgumentException() }

    /* return a single frame from a stream of bytes */
    override def eventPipeline = { frame: ByteString =>
      ctx.singleEvent(SyslogFrame(frame))
    }
  }
}

/**
 * Consume a SyslogFrames object containing a sequence of frames, and emit a sequence
 * of SyslogEvents.
 */
class ProcessFrames extends SymmetricPipelineStage[SyslogContext, SyslogProcessingEvent, SyslogProcessingEvent] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogProcessingEvent, SyslogProcessingEvent] {

    /* we don't process commands */
    override def commandPipeline = {messages => throw new IllegalArgumentException() }

    /* return a single message from a frame */
    override def eventPipeline = {
      case SyslogFrame(frame) =>
        val event = try {
          processFrame(frame)
        } catch {
          case cause: Throwable => new SyslogProcessingFailure(cause)
        }
        ctx.singleEvent(event)
      case event: SyslogProcessingEvent =>
        ctx.singleEvent(event)
    }

    /* process a single frame */
    def processFrame(frame: ByteString): SyslogMessage = {
      val iterator = frame.iterator

      /* process the PRI */
      val priority: Priority = {
        val open = iterator.getByte
        val priority = makeAsciiString(getUntil(iterator, '>'))
        val close = iterator.getByte
        if (open != '<') throw new IllegalArgumentException("PRIVAL must start with <")
        if (close != '>') throw new IllegalArgumentException("PRIVAL must end with >")
        Priority.parseString(priority)
      }

      /* process the VERSION */
      val version: Option[Int] = {
        val version = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException("unexpected data after PRIVAL")
        if (version.length == 0)
          None
        else
          Some(version.toInt)
      }

      /* process the TIMESTAMP */
      val timestamp: DateTime = {
        iterator.head.toChar match {
          case '-' =>
            DateTime.now()
          case head if head.isDigit =>
            val Array(year, month, day) = makeAsciiString(getUntil(iterator, 'T')).split("-").map(_.toInt)
            if (iterator.getByte != 'T') throw new IllegalArgumentException("invalid TIMESTAMP format")
            val Array(hour, minute, second) = makeAsciiString(getUntil(iterator, byte => byte == 'Z' || byte == '-' || byte == '+')).split(":").map(_.toFloat)
            val millis = (second - second.floor) * 1000
            val tz: DateTimeZone = iterator.getByte match {
              case plusOrMinus if plusOrMinus == '-' || plusOrMinus == '+' =>
                val Array(tzhour, tzminute) = makeAsciiString(getUntil(iterator, ' ')).split(":").map(_.toInt)
                CachedDateTimeZone.forZone(DateTimeZone.forOffsetHoursMinutes(if (plusOrMinus == '-') -tzhour else tzhour, tzminute))
              case 'Z' =>
                DateTimeZone.UTC
              case _ => throw new IllegalArgumentException("invalid TIMESTAMP format")
            }
            if (iterator.getByte != ' ') throw new IllegalArgumentException("invalid TIMESTAMP format")
            new DateTime(year, month, day, hour.floor.toInt, minute.floor.toInt, second.floor.toInt, millis.floor.toInt, tz)
          case _ =>
            DateTime.now()
        }
      }

      /* process the HOSTNAME */
      val hostname: String = {
        val hostname = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException("failed to parse HOSTNAME")
        hostname
      }

      /* process the APP-NAME */
      val appName: Option[String] = if (version.isDefined) {
        val appname = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException("failed to parse APP-NAME")
        Some(appname)
      } else None

      /* process the PROCID */
      val procId: Option[String] = if (version.isDefined) {
        val procid = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException("failed to parse PROCID")
        Some(procid)
      } else None

      /* process MSGID */
      val msgId: Option[String] = if (version.isDefined) {
        val msgId = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException("failed to parse MSGID")
        Some(msgId)
      } else None

      /* process STRUCTURED-DATA */
      val elements: Map[SDIdentifier,SDElement] = if (version.isDefined) {
        val elements = scala.collection.mutable.HashMap[SDIdentifier, SDElement]()
        if (!iterator.hasNext) new SyslogProcessingFailure(new IllegalArgumentException("failed to parse STRUCTURED-DATA"))
        while (iterator.hasNext && (iterator.getByte match {
          case ' ' => false
          case '-' =>
            if (iterator.getByte != ' ') throw new IllegalArgumentException("failed to parse STRUCTURED-DATA")
            false
          case '[' => true
          case _ => throw new IllegalArgumentException("failed to parse STRUCTURED-DATA") })) {
            val element = processStructuredData(iterator)
            elements += element
        }
        elements.toMap
      } else Map.empty

      /* process MSG */
      val msg: Option[String] = {
        if (iterator.hasNext) {
          Some(makeUtf8String(iterator))
        } else None
      }

      SyslogMessage(hostname, timestamp, priority, elements, appName, procId, msgId, msg)
    }

    /* process a single structured data instance */
    def processStructuredData(iterator: ByteIterator): (SDIdentifier, SDElement) = {
      val id: SDIdentifier = {
        val id = makeAsciiString(getUntil(iterator, ' '))
        val (name,enterpriseId) = {
          val (name,enterpriseId) = id.span(char => char != '@')
          if (enterpriseId.length > 0) (name, Some(enterpriseId.tail)) else (name, None)
        }
        SDIdentifier(name, enterpriseId)
      }
      val params = scala.collection.mutable.HashMap[String,String]()
      while (iterator.hasNext && (iterator.getByte match {
        case ' ' => true
        case ']' => false
        case _ => throw new IllegalArgumentException("failed to parse SD-ELEMENT")
      })) {
        val param = processParam(iterator)
        params += param
      }
      (id, SDElement(id, params.toMap))
    }

    def processParam(iterator: ByteIterator): (String,String) = {
      val name = makeAsciiString(getUntil(iterator, '='))
      if (iterator.getByte != '=') throw new IllegalArgumentException("failed to parse SD-PARAM")
      val value = processParamValue(iterator)
      (name, value)
    }

    def processParamValue(iterator: ByteIterator): String = {
      if (iterator.getByte != '"') throw new IllegalArgumentException("failed to parse PARAM-VALUE")
      val bb = ByteString.newBuilder
      var escaped = false
      while (iterator.hasNext) {
        iterator.getByte match {
          case '\\' =>
            if (escaped) {
              bb += '\\'
              escaped = false
            } else
              escaped = true
          case ']' =>
            if (escaped) {
              bb += ']'
              escaped = false
            } else
              throw new IllegalArgumentException("failed to parse PARAM-VALUE")
          case '"' =>
            if (escaped) {
              bb += '"'
              escaped = false
            } else {
              val value = new String(bb.result().toArray, SyslogProcessingOps.UTF_8_CHARSET)
              return value
            }
          case b =>
            bb += b
        }
      }
      throw new IllegalArgumentException("failed to parse PARAM-VALUE")
    }
  }
}


/**
 * Consume a SyslogMessage and emit a SyslogEvent object containing the BierEvent.
 */
class ProcessMessage extends SymmetricPipelineStage[SyslogContext, SyslogProcessingEvent, SyslogProcessingEvent] with SyslogProcessingOps {
  import com.syntaxjockey.terane.indexer.bier.BierEvent._
  import com.syntaxjockey.terane.indexer.bier.EventValue
  import scala.util.{Success, Failure}

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogProcessingEvent, SyslogProcessingEvent] {

    /* we don't process commands */
    override def commandPipeline = {frames => throw new IllegalArgumentException() }

    /* return a single frame from a stream of bytes */
    override def eventPipeline = {
      case message: SyslogMessage =>
        val event = try {
          processMessage(message)
        } catch {
          case cause: Throwable => new SyslogProcessingFailure(cause)
        }
        ctx.singleEvent(event)
      case event: SyslogProcessingEvent =>
        ctx.singleEvent(event)
    }

    def processMessage(message: SyslogMessage): SyslogEvent = {

      // add standard RFC5424 fields to event
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      var event = BierEvent(Some(id)) ++ Seq(
        "origin" -> Hostname(new Name(message.origin)),
        "timestamp" -> Datetime(message.timestamp),
        "facility" -> Literal(message.priority.facilityString),
        "severity" -> Literal(message.priority.severityString)
      )
      if (message.appName.isDefined)
        event = event + ("appname" -> Literal(message.appName.get))
      if (message.procId.isDefined)
        event = event + ("procid" -> Literal(message.procId.get))
      if (message.msgId.isDefined)
        event = event + ("msgid" -> Literal(message.msgId.get))
      if (message.message.isDefined)
        event = event + ("message" -> Text(message.message.get))

      // if there is a schema SDElement, then add fields to ctx
      message.elements.get(SDIdentifier.SCHEMA) match {
        case Some(SDElement(_, params)) =>
          val schema: Map[String,FieldIdentifier] = params.map { case (ident, fieldspec) =>
            FieldIdentifier.fromSpec(fieldspec) match {
              case Success(fieldId) =>
                ident -> fieldId
              case Failure(ex) =>
                throw ex
            }
          }.toMap
          // FIXME: what do we do if ident has already been used?
          ctx.schema = ctx.schema ++ schema
          ctx.getLogger.debug("updated schema for this connection: {}", ctx.schema)
        case None =>  // do nothing
      }

      // if there is a values SDElement, then add fields to event
      val values: Seq[KeyValue] = message.elements.get(SDIdentifier.VALUES) match {
        case Some(SDElement(_, params)) =>
          params.flatMap { case (ident, value) =>
            ctx.schema.get(ident) match {
              case Some(fieldId: FieldIdentifier) =>
                Some(processField(fieldId, value))
              case None =>
                None
            }
          }.toSeq
        case None =>
          Seq.empty
      }

      SyslogEvent(event ++ values)
    }

    def processField(fieldId: FieldIdentifier, value: String): KeyValue = {
      fieldId.fieldType match {
        case DataType.TEXT =>
          fieldId -> EventValue(text = Some(Text(value)))
        case DataType.LITERAL =>
          fieldId -> EventValue(literal = Some(Literal(value)))
        case DataType.INTEGER =>
          fieldId -> EventValue(integer = Some(Integer(value)))
        case DataType.FLOAT =>
          fieldId -> EventValue(float = Some(Float(value)))
        case DataType.DATETIME =>
          fieldId -> EventValue(datetime = Some(Datetime(value)))
        case DataType.HOSTNAME =>
          fieldId -> EventValue(hostname = Some(Hostname(value)))
        case DataType.ADDRESS =>
          fieldId -> EventValue(address = Some(Address(value)))
      }
    }
  }
}