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
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.x.discovery.{ServiceCache, ServiceInstance, ServiceDiscoveryBuilder}
import org.apache.curator.x.discovery.details.ServiceCacheListener
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConversions._
import scala.util.Random

import com.syntaxjockey.terane.indexer.IndexerConfig

/**
 * Gossiper is responsible for disseminating gossip information from the parent actor
 * to all registered peers of the same gossipType in the specified zookeeper services path.
 * Gossiper periodically sends a RequestGossip message to its parent, and expects a Gossip
 * message back containing the current state of the node.
 */
class Gossiper(zookeeper: CuratorFramework, servicesPath: String, gossipType: String, interval: FiniteDuration) extends Actor with ActorLogging {
  import Gossiper._
  import context.dispatcher

  /* describe ourselves */
  val selfAddress = Cluster(context.system).selfAddress
  val serviceInstance = ServiceInstance.builder[Void]()
      .name(gossipType)
      .id(IndexerConfig(context.system).settings.nodeId.toString)
      .address(self.path.toStringWithAddress(selfAddress))
      .build()

  /* start service discovery */
  val serviceDiscovery = ServiceDiscoveryBuilder
    .builder(classOf[Void])
    .client(zookeeper)
    .basePath(servicesPath)
    .thisInstance(serviceInstance)
    .build()
  serviceDiscovery.start()

  /* start the service cache */
  val serviceCache = serviceDiscovery.serviceCacheBuilder().name(gossipType).build()
  serviceCache.addListener(new GossipServiceListener(serviceCache, self))
  serviceCache.start()

  // state
  var currentPeers = GossipPeers(Seq.empty)
  var currentData: Option[Serializable] = None
  var requestGossip: Option[Cancellable] = Some(context.system.scheduler.schedule(interval, interval, self, RequestGossip))

  def receive = {

    /* peers have been added or removed */
    case PeersChanged =>
      currentPeers = GossipPeers(serviceCache.getInstances.map(instance => context.actorSelection(instance.getAddress)))
      log.debug("gossip peers changed, new map is {}", currentPeers.peers)

    /* peer gossip */
    case GossipPayload(gossip) =>
      log.debug("received gossip payload from {}", sender.toString())
      context.parent ! Gossip(gossip)

    case RequestGossip =>
      context.parent ! RequestGossip

    /* state from parent */
    case Gossip(data) =>
      if (!currentPeers.peers.isEmpty) {
        val randomPeer = currentPeers.peers(Random.nextInt(currentPeers.peers.length))
        randomPeer ! GossipPayload(data)
      } else log.debug("ignoring gossip, no remote peers to share with")

    /* connection state changed */
    case StateChanged(state) =>
      log.debug("noticed connection state change")
  }

  override def postStop() {
    for (cancellable <- requestGossip)
      cancellable.cancel()
    serviceCache.close()
    serviceDiscovery.close()
  }
}

object Gossiper {
  def props(zookeeper: CuratorFramework, servicesPath: String, gossipType: String, interval: FiniteDuration) = {
    Props(classOf[Gossiper], zookeeper, servicesPath, gossipType, interval)
  }

  case class GossipPayload(gossip: Serializable)
  case class GossipPeers(peers: Seq[ActorSelection])
  case object SendGossip
  case object PeersChanged
}

case class Gossip(data: Serializable)
case object RequestGossip

/**
 * Listens for changes to the service cache, notifies the Gossiper if peers have changed or
 * the connection state has changed.
 */
class GossipServiceListener(cache: ServiceCache[Void], manager: ActorRef) extends ServiceCacheListener {
  import Gossiper._
  def cacheChanged() {
    manager ! PeersChanged
  }
  def stateChanged(client: CuratorFramework, state: ConnectionState) {
    manager ! StateChanged(state)
  }
}
