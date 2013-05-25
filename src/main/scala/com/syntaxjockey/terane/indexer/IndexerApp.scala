package com.syntaxjockey.terane.indexer

import akka.actor.{Props, ActorSystem}
import com.syntaxjockey.terane.indexer.http.HttpServer
import com.syntaxjockey.terane.indexer.syslog.SyslogUdpSource

/**
 *
 */
object IndexerApp extends App {
  val system = ActorSystem("terane-indexer")
  val eventRouter = system.actorOf(Props[EventRouter], "event-router")
  val syslogSource = system.actorOf(Props(new SyslogUdpSource(eventRouter)), "syslog-udp-source")
  val httpApi = system.actorOf(Props(new HttpServer(eventRouter)), "http-api")
}
