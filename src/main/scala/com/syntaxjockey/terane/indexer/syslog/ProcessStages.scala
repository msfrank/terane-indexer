package com.syntaxjockey.terane.indexer.syslog

import akka.io.{SymmetricPipePair, PipelineContext, SymmetricPipelineStage}
import akka.util.{ByteStringBuilder, ByteIterator, ByteString}
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.tz.CachedDateTimeZone
import java.nio.charset.Charset

class SyslogContext extends PipelineContext

object ProcessBody {
  val US_ASCII_CHARSET = Charset.forName("US-ASCII")
  val UTF_8_CHARSET = Charset.forName("UTF-8")
}

class ProcessBody extends SymmetricPipelineStage[SyslogContext, Message, ByteString] {

  override def apply(ctx: SyslogContext) = new SymmetricPipePair[Message, ByteString] {

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
      new String(iterator.toArray, ProcessBody.US_ASCII_CHARSET)
    }

    def makeUtf8String(iterator: ByteIterator): String = {
      new String(iterator.toArray, ProcessBody.UTF_8_CHARSET)
    }

    override def commandPipeline = {message: Message => throw new IllegalArgumentException() }
    override def eventPipeline = { body: ByteString =>
      val iterator = body.iterator

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
        val sdata = scala.collection.mutable.HashMap[SDIdentifier, SDElement]()
        while (iterator.getByte match {
        case '-' | ' ' => false
        case '[' => true
        case _ => throw new IllegalArgumentException() }) {
          sdata += processStructuredData(iterator)
        }
        sdata.toMap
      } else Map.empty

      /* process MSG */
      val msg: Option[String] = {
        if (iterator.hasNext) {
          if (iterator.getByte != ' ') throw new IllegalArgumentException()
          Some(makeUtf8String(iterator))
        } else None
      }

      ctx.singleEvent(Message(hostname, timestamp, priority, elements, appName, procId, msgId, msg))
    }

    def processStructuredData(iterator: ByteIterator): (SDIdentifier, SDElement) = {
      val element = getUntil(iterator, ']')
      val id: SDIdentifier = {
        val id = makeAsciiString(getUntil(element, ' '))
        val (name,number) = id.span(char => char != '@')
        if (element.getByte != ' ') throw new IllegalArgumentException()
        SDIdentifier(name, if (number.length == 0) None else Some(number.toInt))
      }
      val params = scala.collection.mutable.HashMap[String,String]()
      while (element.hasNext) {
        params += processParam(element)
        element.getByte match {
          case ']' => return (id, SDElement(id, params.toMap))
          case byte if byte != ' ' => throw new IllegalArgumentException()
        }
      }
      throw new IllegalArgumentException()
    }

    def processParam(iterator: ByteIterator): (String,String) = {
      val name = makeAsciiString(getUntil(iterator, '='))
      if (iterator.getByte != '=') throw new IllegalArgumentException()
      (name, processParamValue(iterator))
    }

    def processParamValue(iterator: ByteIterator): String = {
      if (iterator.getByte != '"') throw new IllegalArgumentException()
      val sb = new ByteStringBuilder()
      var escaped = false
      while (iterator.hasNext) {
        iterator.getByte match {
          case '\\' =>
            if (escaped) {
              sb.putByte('\\')
              escaped = false
            } else
              escaped = true
          case ']' =>
            if (escaped) {
              sb.putByte(']')
              escaped = false
            } else
              throw new IllegalArgumentException()
          case '"' =>
            if (escaped) {
              sb.putByte('"')
              escaped = false
            } else
              return new String(sb.result().toArray, "UTF-8")
          case _ =>
            sb.putByte(_)
        }
      }
      throw new IllegalArgumentException()
    }
  }
}
