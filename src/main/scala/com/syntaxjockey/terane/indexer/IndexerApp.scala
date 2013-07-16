package com.syntaxjockey.terane.indexer

import akka.actor.{ActorRef, Props, ActorSystem}
import com.typesafe.config._
import scala.collection.JavaConversions._

import com.syntaxjockey.terane.indexer.http.HttpServer
import com.syntaxjockey.terane.indexer.syslog.SyslogUdpSource
import scala.Some
import com.syntaxjockey.terane.indexer.cassandra.CassandraClient
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperClient

/**
 * Indexer application entry point
 */
object IndexerApp extends App {

  val config = ConfigFactory.load()
  val system = ActorSystem("terane-indexer")

  /* synchronously connect to zookeeper and cassandra before starting actors */
  val zookeeper = new ZookeeperClient(config.getConfig("terane.zookeeper"))
  val cassandra = new CassandraClient(config.getConfig("terane.cassandra"))

  /* start the sinks */
  val eventRouter = system.actorOf(Props(new EventRouter(zookeeper, cassandra)), "event-router")

  /* start the sources */
  val sources: Seq[ActorRef] = if (config.hasPath("terane.sources"))
    config.getConfig("terane.sources").root()
      .filter { entry => entry._2.valueType() == ConfigValueType.OBJECT }
      .map { entry => system.actorOf(Props(new SyslogUdpSource(entry._2.asInstanceOf[ConfigObject].toConfig, eventRouter)), "source-" + entry._1)
    }.toSeq
  else Seq.empty

  /* start the api */
  val httpApi = if (config.hasPath("terane.http"))
    Some(system.actorOf(Props(new HttpServer(config.getConfig("terane.http"), eventRouter)), "http-api"))
  else None

  // FIXME: add appropriate shutdown logic
}
