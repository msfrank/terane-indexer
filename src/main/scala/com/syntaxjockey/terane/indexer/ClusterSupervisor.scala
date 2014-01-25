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

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import org.apache.curator.x.discovery.{ServiceDiscoveryBuilder, ServiceInstance}
import scala.concurrent.Future
import scala.collection.immutable.Seq
import scala.collection.JavaConversions._
import java.util.UUID

import com.syntaxjockey.terane.indexer.ClusterSupervisor.{ClusterState,ClusterData}
import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.source.SourceSettings
import com.syntaxjockey.terane.indexer.sink.SinkSettings
import com.syntaxjockey.terane.indexer.route.{RouteContext, EventRouter, RouteSettings}
import com.syntaxjockey.terane.indexer.http.HttpServer
import akka.routing.SmallestMailboxRouter

/**
 * Top level supervisor actor.
 */
class ClusterSupervisor extends Actor with ActorLogging with FSM[ClusterState,ClusterData] {
  import ClusterSupervisor._

  val settings = IndexerConfig(context.system).settings

  val minimumSize = 1

  /* start the event router */
  // FIXME: get router config from settings
  val eventRouter = context.actorOf(EventRouter.props().withRouter(SmallestMailboxRouter(nrOfInstances = 5)), "event-router")

  /* start the toplevel domain object managers */
  val sources = context.actorOf(SourceManager.props(self, eventRouter), "source-manager")
  val sinks = context.actorOf(SinkManager.props(self), "sink-manager")
  val routes = context.actorOf(RouteManager.props(self, eventRouter), "route-manager")
  val queries = context.actorOf(SearchManager.props(self), "search-manager")

  /* start the HTTP service if configured */
  val http = settings.http match {
    case Some(httpSettings) =>
      Some(context.actorOf(HttpServer.props(self, httpSettings), "http"))
    case None =>
      None
  }

  val cluster = Cluster(context.system)

  /* */
  cluster.registerOnMemberUp {
    self ! ClusterReady
  }

  /* subscribe to cluster events */
  cluster.subscribe(self, classOf[ClusterDomainEvent])

  /* monitor the zookeeper client connection state */
  val zookeeper = Zookeeper(context.system).client

  /* the ActorSystem address, used to uniquely identify and address a node */
  val address = cluster.selfAddress.toString

  /* create the service discovery */
  val serviceInstance = ServiceInstance.builder[Void]()
    .name("node")
    .id(settings.nodeId.toString)
    .address(address)
    .build()
  val serviceDiscovery = ServiceDiscoveryBuilder
    .builder(classOf[Void])
    .client(zookeeper)
    .basePath("/services")
    .thisInstance(serviceInstance)
    .build()

  override def preStart() {
    /* register in zookeeper and perform initial join */
    serviceDiscovery.start()
    import context.dispatcher
    Future {
      val seeds = Seq.empty ++ serviceDiscovery.queryForInstances("node").map(instance => AddressFromURIString(instance.getAddress))
      if (seeds.length > 0)
        cluster.joinSeedNodes(seeds.take(3))
    }
  }

  startWith(Connecting, EmptyCluster)

  when(Connecting) {
    case Event(ClusterReady, _) =>
      cluster.sendCurrentClusterState(self)
      goto(Ready) using EmptyCluster
    case Event(event: ClusterDomainEvent, _) =>
      stay()  // ignore event
  }

  when(Ready) {
    case Event(state: CurrentClusterState, _) =>
      if (state.leader.get == cluster.selfAddress)
        goto(ClusterLeader) using ClusterUp(state)
      else
        goto(ClusterWorker) using ClusterUp(state)
    case Event(event: ClusterDomainEvent, _) =>
      stay()  // ignore other cluster domain events
  }

  onTransition {
    case Ready -> ClusterLeader =>
      context.system.eventStream.publish(NodeBecomesLeader)
    case Ready -> ClusterWorker =>
      context.system.eventStream.publish(NodeBecomesWorker)
  }

  when(ClusterLeader) {
    case Event(LeaderChanged(Some(leader)), ClusterUp(state)) if leader != cluster.selfAddress =>
      goto(ClusterWorker)
    case Event(state: CurrentClusterState, _) =>
      stay() using ClusterUp(state)
    case Event(op: SearchOperation, _) =>
      queries forward op
      stay()
    case Event(op: SourceOperation, _) =>
      sources forward op
      stay()
    case Event(op: SinkOperation, _) =>
      sinks forward op
      stay()
    case Event(op: RouteOperation, _) =>
      routes forward op
      stay()
    case Event(LeaderOperation(caller, op), _) =>
      self.tell(op, caller)
      stay()
    case Event(event: ClusterDomainEvent, _) =>
      stay()  // ignore other cluster domain events
  }

  onTransition {
    case ClusterLeader -> ClusterWorker =>
      context.system.eventStream.publish(NodeBecomesWorker)
    case ClusterWorker -> ClusterLeader =>
      context.system.eventStream.publish(NodeBecomesLeader)
  }

  when(ClusterWorker) {
    case Event(LeaderChanged(Some(leader)), ClusterUp(state)) if leader == cluster.selfAddress =>
      goto(ClusterLeader)
    case Event(state: CurrentClusterState, _) =>
      stay() using ClusterUp(state)
    case Event(op: SinkOperation with CanPerformAnywhere, _) =>
      sinks forward op
      stay()
    case Event(op: LeaderOperation, ClusterUp(state)) =>
      val selection = context.actorSelection(self.path.toStringWithAddress(state.leader.get))
      selection ! op
      stay()
    case Event(op: ClusterOperation, ClusterUp(state)) =>
      stay() replying NodeIsNotLeader(op, state.leader.get)
    case Event(event: ClusterDomainEvent, _) =>
      stay()  // ignore other cluster domain events
  }

  initialize()
}

object ClusterSupervisor {
  def props() = Props[ClusterSupervisor]

  sealed trait ClusterState
  case object Connecting extends ClusterState
  case object Ready extends ClusterState
  case object ClusterLeader extends ClusterState
  case object ClusterWorker extends ClusterState
  case object Disconnected extends ClusterState

  sealed trait ClusterData
  case object EmptyCluster extends ClusterData
  case class ClusterUp(state: CurrentClusterState) extends ClusterData

  case object ClusterReady
}

sealed trait SupervisorEvent
case object NodeBecomesLeader extends SupervisorEvent
case object NodeBecomesWorker extends SupervisorEvent

case class NodeIsNotLeader(op: ClusterOperation, leader: Address)
case class LeaderOperation(caller: ActorRef, op: ClusterOperation)

/*
 * base traits for cluster operations
 */
sealed trait ClusterOperation
sealed trait ClusterCommand extends ClusterOperation
sealed trait ClusterQuery extends ClusterOperation
trait MustPerformOnLeader
trait CanPerformAnywhere

/*
 * Source operations
 */
sealed trait SourceOperation
sealed trait SourceCommand extends SourceOperation with ClusterCommand
sealed trait SourceQuery extends SourceOperation with ClusterQuery
case class CreateSource(settings: SourceSettings) extends SourceCommand with MustPerformOnLeader
case class DeleteSource(name: String) extends SourceCommand with MustPerformOnLeader
case class DescribeSource(name: String) extends SourceQuery with CanPerformAnywhere
case object EnumerateSources extends SourceQuery with CanPerformAnywhere

/*
 * Sink operations
 */
sealed trait SinkOperation
sealed trait SinkCommand extends SinkOperation with ClusterCommand
sealed trait SinkQuery extends SinkOperation with ClusterQuery
case class CreateSink(settings: SinkSettings) extends SinkCommand with MustPerformOnLeader
case class DeleteSink(name: String) extends SinkCommand with MustPerformOnLeader
case class DescribeSink(name: String) extends SinkQuery with CanPerformAnywhere
case object EnumerateSinks extends SinkQuery with CanPerformAnywhere

/*
 * Route operations
 */
sealed trait RouteOperation
sealed trait RouteCommand extends RouteOperation with ClusterCommand
sealed trait RouteQuery extends RouteOperation with ClusterQuery
case class CreateRoute(context: RouteContext, position: Option[Int]) extends RouteCommand with MustPerformOnLeader
case class ReplaceRoute(context: RouteContext, position: Int) extends RouteCommand with MustPerformOnLeader
case class DeleteRoute(position: Option[Int]) extends RouteCommand with MustPerformOnLeader
case object EnumerateRoutes extends RouteQuery with CanPerformAnywhere

/*
 * Search operations
 */
sealed trait SearchOperation
sealed trait SearchCommand extends SearchOperation with ClusterCommand
sealed trait SearchQuery extends SearchOperation with ClusterQuery
case class CreateQuery(query: String, store: String, fields: Option[Set[String]], sortBy: Option[List[FieldIdentifier]], limit: Option[Int], reverse: Option[Boolean]) extends SearchCommand with CanPerformAnywhere
case class DeleteQuery(id: UUID) extends SearchCommand with CanPerformAnywhere
case class GetEvents(offset: Option[Int] = None, limit: Option[Int] = None) extends SearchQuery with CanPerformAnywhere
case class DescribeQuery(id: UUID) extends SearchQuery with CanPerformAnywhere
case object EnumerateQueries extends SearchQuery with CanPerformAnywhere
