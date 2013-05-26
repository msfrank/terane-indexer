package com.syntaxjockey.terane.indexer.syslog

import com.syntaxjockey.terane.indexer.bier.Event
import com.netflix.astyanax.util.TimeUUIDUtils

/**
 *
 */
trait SyslogReceiver {

  def message2event(message: Message): Event = {
    val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
    val event = Event(Some(id))
    event("origin") = Event.Value(literal = Some(Set(message.origin)))
    event("timestamp") = Event.Value(datetime = Some(message.timestamp))
    event("facility") = Event.Value(literal = Some(Set(message.priority.facilityString)))
    event("severity") = Event.Value(literal = Some(Set(message.priority.severityString)))
    if (message.appName.isDefined)
      event("appname") = Event.Value(literal = Some(Set(message.appName.get)))
    if (message.procId.isDefined)
      event("procid") = Event.Value(literal = Some(Set(message.procId.get)))
    if (message.msgId.isDefined)
      event("msgid") = Event.Value(literal = Some(Set(message.msgId.get)))
    if (message.message.isDefined)
      event("message") = Event.Value(text = Some(message.message.get))
    event
  }
}
