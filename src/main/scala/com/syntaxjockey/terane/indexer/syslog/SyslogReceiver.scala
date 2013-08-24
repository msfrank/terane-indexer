package com.syntaxjockey.terane.indexer.syslog

import com.netflix.astyanax.util.TimeUUIDUtils
import org.xbill.DNS.Name

import com.syntaxjockey.terane.indexer.bier.Event
import com.syntaxjockey.terane.indexer.bier.Event._
import com.syntaxjockey.terane.indexer.bier.datatypes._

/**
 *
 */
trait SyslogReceiver {

  implicit def message2event(message: Message): Event = {
    val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
    var event = Event(Some(id)) ++ Seq(
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
    event
  }
}
