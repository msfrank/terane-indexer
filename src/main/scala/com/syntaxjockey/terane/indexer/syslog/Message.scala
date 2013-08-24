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

import org.joda.time.DateTime

case class Priority(facility: Int, severity: Int) {
  def facilityString = Priority.facilityToString(facility)
  def severityString = Priority.severityToString(severity)
  override def toString: String = "%s|%s".format(facilityString, severityString)
}

case object Priority {
  val facilityToString = Map(
    0 -> "kern",
    1 -> "user",
    2 -> "mail",
    3 -> "daemon",
    4 -> "auth",
    5 -> "syslog",
    6 -> "lpr",
    7 -> "news",
    8 -> "uucp",
    9 -> "cron",
    10 -> "authpriv",
    11 -> "ftp",
    12 -> "ntp",
    13 -> "auth",
    14 -> "auth",
    15 -> "cron",
    16 -> "local0",
    17 -> "local1",
    18 -> "local2",
    19 -> "local3",
    20 -> "local4",
    21 -> "local5",
    22 -> "local6",
    23 -> "local7")

  val severityToString = Map(
    0 -> "emerg",
    1 -> "alert",
    2 -> "crit",
    3 -> "err",
    4 -> "warning",
    5 -> "notice",
    6 -> "info",
    7 -> "debug")

  def parseString(s: String): Priority = {
    if (s.length > 3) throw new IllegalArgumentException("PRIVAL may not be longer than 3 bytes")
    if (s == "0")
      Priority(0, 0)
    else {
      if (s(0) == '0') throw new IllegalArgumentException("PRIVAL may not start with a leading zero")
      val prival: Int = s.toInt
      Priority(prival / 8, prival % 8)
    }
  }
}

case class SDIdentifier(name: String, enterpriseId: Option[String]) {
  def reserved = enterpriseId.isEmpty
  override def toString: String = if (reserved) name else "%s@%s".format(name, enterpriseId.get)
}

case object SDIdentifier {
  val TIME_QUALITY = SDIdentifier("timeQuality", None)
  val ORIGIN = SDIdentifier("origin", None)
  val META = SDIdentifier("meta", None)
}

case class SDElement(id: SDIdentifier, params: Map[String,String])

case object SDElement {
  val PARAM_TZ_KNOWN = "tzKnown"
  val PARAM_IS_SYNCED = "isSynced"
  val PARAM_SYNC_ACCURACY = "syncAccuracy"
  val PARAM_IP = "ip"
  val PARAM_ENTERPRISE_ID = "enterpriseId"
  val PARAM_SOFTWARE = "software"
  val PARAM_SW_VERSION = "swVersion"
  val PARAM_SEQUENCE_ID = "sequenceId"
  val PARAM_SYS_UPTIME = "sysUptime"
  val PARAM_LANGUAGE = "language"
}

case class Message(
  origin: String,
  timestamp: DateTime,
  priority: Priority,
  elements: Map[SDIdentifier,SDElement],
  appName: Option[String] = None,
  procId: Option[String] = None,
  msgId: Option[String] = None,
  message: Option[String] = None) {

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("%s@%s %s".format(origin, timestamp, priority))
    for (v <- appName)
      sb.append(" appName=%s".format(v))
    for (v <- procId)
      sb.append(" procId=%s".format(v))
    for (v <- msgId)
      sb.append(" msgId=%s".format(v))
    for (element <- elements.values) {
      sb.append(" [%s".format(element.id))
      for ((name,value) <- element.params)
        sb.append(" %s=%s".format(name, value))
      sb.append("]")
    }
    for (v <- message)
      sb.append(" " + v)
    sb.toString()
  }
}
