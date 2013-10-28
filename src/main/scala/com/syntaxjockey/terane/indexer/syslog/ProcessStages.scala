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
class SyslogContext(logging: LoggingAdapter, context: ActorContext) extends PipelineContext with WithinActorContext {
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

case class SyslogFrames(frames: Seq[ByteString], leftover: ByteString)
case class SyslogMessages(messages: Seq[Message], leftover: ByteString)

class UnrecoverableProcessingError(cause: Throwable) extends Exception("unrecoverable processing error", cause)

/**
 * Consume a ByteString containing zero or more framed syslog messages, and emit a
 * SyslogFrames object containing the frames and any leftover unprocessed data.
 */
class ProcessTcp extends SymmetricPipelineStage[SyslogContext, SyslogFrames, ByteString] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogFrames, ByteString] {

    /* we don't process commands */
    override def commandPipeline = {frames => throw new IllegalArgumentException() }

    /* return the framed messages, or throw UnrecoverableProcessingError */
    override def eventPipeline = { body: ByteString =>
      var frames = SyslogFrames(Seq.empty, body)
      var leftover = body
      while (!leftover.isEmpty) {
        try {
          val iterator = leftover.iterator
          val msglenString = makeAsciiString(getUntil(iterator, ' '))
          // process the msglen
          if (!isNonzeroDigit(msglenString(0).toByte))
            throw new IllegalArgumentException("msglen must start with a non-zero digit")
          msglenString.tail.foreach { char =>
            if (!isDigit(char.toByte))
              throw new IllegalArgumentException("msglen must consist only of digits")
          }
          val msglen = msglenString.toInt
          iterator.next()
          val bytesleft = iterator.toByteString
          // process the frame
          if (bytesleft.length < msglen) {
            leftover = ByteString.empty
            frames = SyslogFrames(frames.frames, bytesleft)
          } else {
            val frame = bytesleft.take(msglen)
            leftover = bytesleft.drop(msglen)
            frames = SyslogFrames(frames.frames :+ frame, frames.leftover)
          }
        } catch {
          // if the iterator has no more data
          case ex: NoSuchElementException =>
            leftover = ByteString.empty
            frames = SyslogFrames(frames.frames, leftover)
          // any other exception is considered unrecoverable
          case ex: Throwable =>
            throw new UnrecoverableProcessingError(ex)
        }
      }
      ctx.singleEvent(frames)
    }

    def isDigit(byte: Byte): Boolean = byte >= 0x30 && byte <= 0x39

    def isNonzeroDigit(byte: Byte): Boolean = byte >= 0x31 && byte <= 0x39
  }
}

/**
 * Consume a ByteString containing one framed syslog message, and emit a SyslogFrames
 * object containing the frame.
 */
class ProcessUdp extends SymmetricPipelineStage[SyslogContext, SyslogFrames, ByteString] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogFrames, ByteString] {

    /* we don't process commands */
    override def commandPipeline = {frames => throw new IllegalArgumentException() }

    /* return a singled framed message */
    override def eventPipeline = { frame: ByteString =>
      ctx.singleEvent(SyslogFrames(Seq(frame), ByteString.empty))
    }
  }
}

/**
 * Consume a SyslogFrames object containing a sequence of frames, and emit a SyslogMessages
 * object containing the corresponding sequence of messages.
 */
class ProcessFrames extends SymmetricPipelineStage[SyslogContext, SyslogMessages, SyslogFrames] with SyslogProcessingOps {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[SyslogMessages, SyslogFrames] {

    /* we don't process commands */
    override def commandPipeline = {messages => throw new IllegalArgumentException() }

    /* return messages from a sequence of frames */
    override def eventPipeline = { case SyslogFrames(frames, leftover) =>
      try {
        val messages = frames.map(processFrame)
        ctx.singleEvent(SyslogMessages(messages, leftover))
      } catch {
        case ex: Throwable => throw new UnrecoverableProcessingError(ex)
      }
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
        if (iterator.getByte != ' ') throw new IllegalArgumentException("Unexpected data after PRIVAL")
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
            if (iterator.getByte != 'T') throw new IllegalArgumentException()
            val Array(hour, minute, second) = makeAsciiString(getUntil(iterator, byte => byte == 'Z' || byte == '-' || byte == '+')).split(":").map(_.toFloat)
            val millis = (second - second.floor) * 1000
            val tz: DateTimeZone = iterator.getByte match {
              case plusOrMinus if plusOrMinus == '-' || plusOrMinus == '+' =>
                val Array(tzhour, tzminute) = makeAsciiString(getUntil(iterator, ' ')).split(":").map(_.toInt)
                CachedDateTimeZone.forZone(DateTimeZone.forOffsetHoursMinutes(
                  if (plusOrMinus == '-') -tzhour else tzhour,
                  tzminute))
              case 'Z' =>
                DateTimeZone.UTC
              case _ => throw new IllegalArgumentException("")
            }
            if (iterator.getByte != ' ') throw new IllegalArgumentException()
            new DateTime(year, month, day, hour.floor.toInt, minute.floor.toInt, second.floor.toInt, millis.floor.toInt, tz)
          case _ =>
            DateTime.now()
        }
      }

      /* process the HOSTNAME */
      val hostname: String = {
        val hostname = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException()
        hostname
      }

      /* process the APP-NAME */
      val appName: Option[String] = if (version.isDefined) {
        val appname = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException()
        Some(appname)
      } else None

      /* process the PROCID */
      val procId: Option[String] = if (version.isDefined) {
        val procid = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException()
        Some(procid)
      } else None

      /* process MSGID */
      val msgId: Option[String] = if (version.isDefined) {
        val msgId = makeAsciiString(getUntil(iterator, ' '))
        if (iterator.getByte != ' ') throw new IllegalArgumentException()
        Some(msgId)
      } else None

      /* process STRUCTURED-DATA */
      val elements: Map[SDIdentifier,SDElement] = if (version.isDefined) {
        val elements = scala.collection.mutable.HashMap[SDIdentifier, SDElement]()
        if (!iterator.hasNext) throw new IllegalArgumentException()
        while (iterator.hasNext && (iterator.getByte match {
        case ' ' => false
        case '-' =>
          if (iterator.getByte != ' ') throw new IllegalArgumentException()
          false
        case '[' => true
        case _ => throw new IllegalArgumentException() })) {
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
        case _ => throw new IllegalArgumentException()
      })) {
        val param = processParam(iterator)
        params += param
      }
      (id, SDElement(id, params.toMap))
    }

    def processParam(iterator: ByteIterator): (String,String) = {
      val name = makeAsciiString(getUntil(iterator, '='))
      if (iterator.getByte != '=') throw new IllegalArgumentException()
      val value = processParamValue(iterator)
      (name, value)
    }

    def processParamValue(iterator: ByteIterator): String = {
      if (iterator.getByte != '"') throw new IllegalArgumentException()
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
              throw new IllegalArgumentException()
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
      throw new IllegalArgumentException()
    }
  }
}
