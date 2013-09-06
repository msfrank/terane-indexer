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

import com.netflix.astyanax.util.TimeUUIDUtils
import org.xbill.DNS.Name

import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer.bier.BierEvent._
import com.syntaxjockey.terane.indexer.bier.datatypes._

/**
 *
 */
trait SyslogReceiver {

  implicit def message2event(message: Message): BierEvent = {
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
    event
  }
}
