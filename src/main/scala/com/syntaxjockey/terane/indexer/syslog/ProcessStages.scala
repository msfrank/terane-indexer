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

package com.syntaxjockey.terane.indexer.syslog

import akka.actor.ActorContext
import akka.io.{SymmetricPipePair, PipelineContext, SymmetricPipelineStage}
import akka.io.TcpPipelineHandler.WithinActorContext
import akka.event.LoggingAdapter
import akka.util.{ByteIterator, ByteString}
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.tz.CachedDateTimeZone
import java.nio.charset.Charset

/**
 * Context when performing pipeline processing.
 */
class SyslogContext(logging: LoggingAdapter, context: ActorContext, val maxMessageSize: Option[Long] = None) extends PipelineContext with WithinActorContext {
  var leftover = ByteString.empty
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

trait SyslogEvent
case class SyslogFrame(frame: ByteString) extends SyslogEvent
case object SyslogIncomplete extends SyslogEvent
class SyslogFailure(cause: Throwable) extends Exception(cause) with SyslogEvent

/**
 * Consume a ByteString containing zero or more framed syslog messages, and emit a
 * SyslogFrames object containing the frames and any leftover unprocessed data.
 */
class ProcessTcp extends SymmetricPipelineStage[SyslogContext, SyslogEvent, ByteString] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogEvent, ByteString] {

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
    def processFrames(data: ByteString): Seq[SyslogEvent] = {
      ctx.leftover = ctx.leftover ++ data
      var events = Seq.empty[SyslogEvent]
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
            events = events ++ Seq(SyslogIncomplete)
          } else {
            ctx.leftover = bytesleft.drop(msglen)
            events = events ++ Seq(SyslogFrame(bytesleft.take(msglen)))
          }
        } catch {
          // if the iterator has no more data
          case ex: NoSuchElementException =>
            finished = true
            events = events ++ Seq(SyslogIncomplete)
          case ex: Throwable =>
            finished = true
            events = events ++ Seq(new SyslogFailure(ex))
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
class ProcessUdp extends SymmetricPipelineStage[SyslogContext, SyslogEvent, ByteString] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogEvent, ByteString] {

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
class ProcessFrames extends SymmetricPipelineStage[SyslogContext, SyslogEvent, SyslogEvent] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogEvent, SyslogEvent] {

    /* we don't process commands */
    override def commandPipeline = {messages => throw new IllegalArgumentException() }

    /* return a single message from a frame */
    override def eventPipeline = {
      case SyslogFrame(frame) =>
        val event = try {
          processFrame(frame)
        } catch {
          case cause: Throwable => new SyslogFailure(cause)
        }
        ctx.singleEvent(event)
      case event: SyslogEvent =>
        ctx.singleEvent(event)
    }

    /* process a single frame */
    def processFrame(frame: ByteString): Message = {
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
        if (!iterator.hasNext) new SyslogFailure(new IllegalArgumentException("failed to parse STRUCTURED-DATA"))
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

      Message(hostname, timestamp, priority, elements, appName, procId, msgId, msg)
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
