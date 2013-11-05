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

import akka.actor.{AddressFromURIString, ActorRef, ActorSystem}
import akka.cluster.Cluster
import com.netflix.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceInstance}
import scala.collection.JavaConversions._
import java.util.UUID

import com.syntaxjockey.terane.indexer.source._
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.http.HttpServer

/**
 * Indexer application entry point
 */
object IndexerApp extends App {

  /* get the runtime configuration */
  val config = IndexerConfig.config

  /* start the actor system */
  val system = ActorSystem("terane-indexer", config)

  val settings = IndexerConfig(system).settings

  /* start the sinks */
  val eventRouter = system.actorOf(EventRouter.props(), "event-router")

  /* start the sources */
  val sources: Seq[ActorRef] = settings.sources.map { case (name: String, sourceSettings: SourceSettings) =>
    sourceSettings match {
      case syslogTcpSourceSettings: SyslogTcpSourceSettings =>
        system.actorOf(SyslogTcpSource.props(syslogTcpSourceSettings, eventRouter), "source-" + name)
      case syslogUdpSourceSettings: SyslogUdpSourceSettings =>
        system.actorOf(SyslogUdpSource.props(syslogUdpSourceSettings, eventRouter), "source-" + name)
    }
  }.toSeq

  /* start the HTTP service if configured */
  val http = settings.http match {
    case Some(httpSettings) =>
      Some(system.actorOf(HttpServer.props(httpSettings, eventRouter), "http-api"))
    case None =>
      None
  }

  /* register as a cluster seed */
  val selfAddress = Cluster(system).selfAddress
  val serviceInstance = ServiceInstance.builder[Void]()
      .name("node")
      .id(UUID.randomUUID().toString)
      .address(selfAddress.toString)
      .build()
  val serviceDiscovery = ServiceDiscoveryBuilder
    .builder(classOf[Void])
    .client(Zookeeper(system).client)
    .basePath("/services")
    .thisInstance(serviceInstance)
    .build()
  serviceDiscovery.start()

  /* join with seed nodes */
  val seeds = serviceDiscovery.queryForInstances("node").take(3).map(instance => AddressFromURIString(instance.getAddress))
  seeds.foreach(seed => Cluster(system).join(seed))

  // FIXME: add appropriate shutdown logic
}
