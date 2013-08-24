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

package com.syntaxjockey.terane.indexer

import akka.actor.{ActorRef, Props, ActorSystem}
import com.typesafe.config._
import scala.collection.JavaConversions._
import scala.Some

import com.syntaxjockey.terane.indexer.syslog.SyslogUdpSource
import com.syntaxjockey.terane.indexer.cassandra.CassandraClient
import com.syntaxjockey.terane.indexer.http.HttpServer

/**
 * Indexer application entry point
 */
object IndexerApp extends App {

  val config = ConfigFactory.load()
  val system = ActorSystem("terane-indexer")

  /* synchronously connect to zookeeper and cassandra before starting actors */
  val cassandra = new CassandraClient(config.getConfig("terane.cassandra"))

  /* start the sinks */
  val eventRouter = system.actorOf(Props(new EventRouter(cassandra)), "event-router")

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
