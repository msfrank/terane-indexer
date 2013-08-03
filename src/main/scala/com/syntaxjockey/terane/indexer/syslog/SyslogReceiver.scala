package com.syntaxjockey.terane.indexer.syslog

import com.netflix.astyanax.util.TimeUUIDUtils
import org.xbill.DNS.Name

import com.syntaxjockey.terane.indexer.bier.Event
import com.syntaxjockey.terane.indexer.bier.Event._

/**
 *
 */
trait SyslogReceiver {

  implicit def message2event(message: Message): Event = {
    val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
    var event = Event(Some(id)) ++ Seq(
      "origin" -> new Name(message.origin),
      "timestamp" -> message.timestamp,
      "facility" -> List(message.priority.facilityString),
      "severity" -> List(message.priority.severityString)
    )
    if (message.appName.isDefined)
      event = event + ("appname" -> List(message.appName.get))
    if (message.procId.isDefined)
      event = event + ("procid" -> List(message.procId.get))
    if (message.msgId.isDefined)
      event = event + ("msgid" -> List(message.msgId.get))
    if (message.message.isDefined)
      event = event + ("message" -> message.message.get)
    event
  }
}
