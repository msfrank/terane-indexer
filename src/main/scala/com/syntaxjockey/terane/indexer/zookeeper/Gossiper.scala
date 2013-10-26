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

package com.syntaxjockey.terane.indexer.zookeeper

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.cluster.Cluster
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.ConnectionState
import com.netflix.curator.x.discovery.{ServiceInstance, ServiceDiscoveryBuilder}
import com.netflix.curator.x.discovery.details.ServiceCacheListener
import java.util.UUID

class Gossiper(gossipType: String, servicesPath: String) extends Actor with ActorLogging {

  val selfAddress = Cluster(context.system).selfAddress
  val serviceInstance = ServiceInstance.builder[Void]()
      .name(gossipType)
      .id(UUID.randomUUID().toString)
      .address(self.path.toStringWithAddress(selfAddress))
      .build()

  /* start service discovery */
  val serviceDiscovery = ServiceDiscoveryBuilder
    .builder(classOf[Void])
    .client(Zookeeper(context.system).client)
    .basePath(servicesPath)
    .thisInstance(serviceInstance)
    .build()
  serviceDiscovery.start()

  /* start the service cache */
  val serviceCache = serviceDiscovery.serviceCacheBuilder().name(gossipType).build()
  serviceCache.addListener(new GossipServiceListener(self))
  serviceCache.start()

  def receive = {
    case _ =>
  }
}

object Gossiper {
  def props(gossipType: String, servicesPath: String) = Props(classOf[Gossiper], gossipType, servicesPath)
}

class GossipServiceListener(manager: ActorRef) extends ServiceCacheListener {
  def cacheChanged() {

  }
  def stateChanged(client: CuratorFramework, state: ConnectionState) {

  }
}
