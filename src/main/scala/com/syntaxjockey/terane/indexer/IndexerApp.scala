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
import com.typesafe.config._
import com.netflix.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceInstance}
import scala.collection.JavaConversions._
import java.util.UUID

import com.syntaxjockey.terane.indexer.syslog.{SyslogTcpSource, SyslogUdpSource}
import com.syntaxjockey.terane.indexer.http.HttpServer
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper

/**
 * Indexer application entry point
 */
object IndexerApp extends App {

  val config = ConfigFactory.load()
  val system = ActorSystem("terane-indexer")

  /* start the sinks */
  val eventRouter = system.actorOf(EventRouter.props(), "event-router")

  /* start the sources */
  val sources: Seq[ActorRef] = if (config.hasPath("terane.sources"))
    config.getConfig("terane.sources").root()
      .filter { case (name: String, configValue: ConfigValue) => configValue.valueType() == ConfigValueType.OBJECT }
      .map { case (name: String, configValue: ConfigValue) =>
        val source = configValue.asInstanceOf[ConfigObject].toConfig
        source.getString("source-type") match {
          case "syslog-udp" =>
            system.actorOf(SyslogUdpSource.props(source, eventRouter), "source-" + name)
          case "syslog-tcp" =>
            system.actorOf(SyslogTcpSource.props(source, eventRouter), "source-" + name)
          case other =>
            throw new Exception("unknown source-type " + other)
        }
      }.toSeq
  else Seq.empty

  /* start the api */
  val httpApi = if (config.hasPath("terane.http"))
    Some(system.actorOf(HttpServer.props(config.getConfig("terane.http"), eventRouter), "http-api"))
  else None

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
