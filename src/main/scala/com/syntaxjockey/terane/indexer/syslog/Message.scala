package com.syntaxjockey.terane.indexer.syslog

import org.joda.time.DateTime

case class Priority(facility: Int, severity: Int) {
  def facilityString = Priority.facilityToString(facility)
  def severityString = Priority.severityToString(severity)
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
  message: Option[String] = None)
