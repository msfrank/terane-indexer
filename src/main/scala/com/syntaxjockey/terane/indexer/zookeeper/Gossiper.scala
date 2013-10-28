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

import akka.actor._
import akka.cluster.Cluster
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.ConnectionState
import com.netflix.curator.x.discovery.{ServiceCache, ServiceInstance, ServiceDiscoveryBuilder}
import com.netflix.curator.x.discovery.details.ServiceCacheListener
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import java.util.UUID

/**
 * Gossiper is responsible for disseminating gossip information from the parent actor
 * to all registered peers of the same gossipType in the specified zookeeper services path
 */
class Gossiper(gossipType: String, servicesPath: String, interval: Duration) extends Actor with ActorLogging {
  import Gossiper._

  /* */
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
  serviceCache.addListener(new GossipServiceListener(serviceCache, self))
  serviceCache.start()

  var currentPeers = GossipPeers(Seq.empty)
  var currentData: Option[Serializable] = None

  def receive = {

    case PeersChanged =>
      log.debug("gossip peers changed")
      currentPeers = GossipPeers(serviceCache.getInstances.map(instance => context.actorSelection(instance.getAddress)))

    /* peer gossip */
    case GossipPayload(gossip) =>
      log.debug("received gossip payload from {}", sender.toString())
      context.parent ! gossip

    case Gossip(data) =>

    /* connection state changed */
    case StateChanged(state) =>
      log.debug("noticed connection state change")
  }

  override def postStop() {
    serviceCache.close()
    serviceDiscovery.close()
  }
}

object Gossiper {
  def props(gossipType: String, servicesPath: String) = Props(classOf[Gossiper], gossipType, servicesPath)

  case class GossipPayload(gossip: Gossip)
  case class GossipPeers(peers: Seq[ActorSelection])
  case object SendGossip
  case object PeersChanged
}

case class Gossip(data: Serializable)

class GossipServiceListener(cache: ServiceCache[Void], manager: ActorRef) extends ServiceCacheListener {
  import Gossiper._
  def cacheChanged() {
    manager ! PeersChanged
  }
  def stateChanged(client: CuratorFramework, state: ConnectionState) {
    manager ! StateChanged(state)
  }
}
