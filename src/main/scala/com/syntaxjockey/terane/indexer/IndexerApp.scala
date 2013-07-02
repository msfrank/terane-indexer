package com.syntaxjockey.terane.indexer

import akka.actor.{Props, ActorSystem}
import com.syntaxjockey.terane.indexer.http.HttpServer
import com.syntaxjockey.terane.indexer.syslog.SyslogUdpSource
import com.syntaxjockey.terane.indexer.metadata.{ZookeeperClient, StoreManager}
import java.util.UUID
import com.typesafe.config.{ConfigFactory, Config}
import com.syntaxjockey.terane.indexer.sink.CassandraClient

/**
 *
 */
object IndexerApp extends App {

  val config = ConfigFactory.load()
  val system = ActorSystem("terane-indexer")

  val zookeeper = new ZookeeperClient(config.getConfig("terane.zookeeper"))
  val cassandra = new CassandraClient(config.getConfig("terane.cassandra"))

  val eventRouter = system.actorOf(Props(new EventRouter(zookeeper, cassandra)), "event-router")
  val syslogSource = system.actorOf(Props(new SyslogUdpSource(eventRouter)), "syslog-udp-source")
  val httpApi = system.actorOf(Props(new HttpServer(eventRouter)), "http-api")

  // FIXME: add appropriate shutdown logic
}
