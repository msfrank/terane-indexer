package com.syntaxjockey.terane.indexer

import akka.actor.{Props, ActorSystem}
import java.net.{InetSocketAddress, Inet4Address}
import com.syntaxjockey.terane.indexer.syslog.SyslogUdpSource

/**
 *
 */
object IndexerApp extends App {
  implicit val system = ActorSystem("echo-server")
  val localAddr = new InetSocketAddress("localhost", 10514)
  val server = system.actorOf(Props(new SyslogUdpSource(localAddr)), name = "syslog-source")
}
