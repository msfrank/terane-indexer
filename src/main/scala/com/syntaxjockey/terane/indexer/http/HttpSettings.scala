package com.syntaxjockey.terane.indexer.http

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit

class HttpSettings(val interface: String, val port: Int, val backlog: Int, val requestTimeout: FiniteDuration)

object HttpSettings {
  def parse(config: Config): HttpSettings = {
    val port = config.getInt("port")
    val interface = config.getString("interface")
    val backlog = config.getInt("backlog")
    val requestTimeout = FiniteDuration(config.getMilliseconds("request-timeout"), TimeUnit.MILLISECONDS)
    new HttpSettings(interface, port, backlog, requestTimeout)
  }
}
