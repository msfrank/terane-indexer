package com.syntaxjockey.terane.indexer

import akka.actor.{Props, ActorSystem}
import com.syntaxjockey.terane.indexer.sink.CassandraSink

/**
 *
 */
object IndexerApp extends App {
  val system = ActorSystem("terane-indexer")
  val sink = system.actorOf(Props(new CassandraSink("store")), "cassandra-sink")
  val eventRouter = system.actorOf(Props(new EventRouter(sink)), "event-router")
}
