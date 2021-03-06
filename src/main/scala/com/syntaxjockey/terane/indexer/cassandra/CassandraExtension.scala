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

package com.syntaxjockey.terane.indexer.cassandra

import akka.actor._
import org.slf4j.LoggerFactory
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{Cluster, AstyanaxContext}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.syntaxjockey.terane.indexer.IndexerConfig

class CassandraManager(_context: AstyanaxContext[Cluster]) extends Actor with ActorLogging {

  log.debug("started cassandra manager")

  def receive = {
    case _ =>
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.warning("restarted cassandra manager")
  }

  override def postStop() {
    _context.shutdown()
    log.debug("stopped cassandra manager")
  }
}

object CassandraManager {
  def props(context: AstyanaxContext[Cluster]) = Props(classOf[CassandraManager], context)
}

class CassandraExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[CassandraExtension])

  val settings = IndexerConfig(system).settings.cassandra

  val configuration = new AstyanaxConfigurationImpl()
    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
  val poolConfiguration = new ConnectionPoolConfigurationImpl(settings.poolName)
    .setPort(settings.port)
    .setMaxConnsPerHost(settings.maxConnsPerHost)
    .setSeeds(settings.servers.mkString(","))
  val connectionPoolMonitor = new CountingConnectionPoolMonitor()
  val context = new AstyanaxContext.Builder()
    .forCluster(settings.clusterName)
    .withAstyanaxConfiguration(configuration)
    .withConnectionPoolConfiguration(poolConfiguration)
    .withConnectionPoolMonitor(connectionPoolMonitor)
    .buildCluster(ThriftFamilyFactory.getInstance())

  log.info("connecting to cluster {}", settings.clusterName)
  context.start()

  val manager = system.actorOf(CassandraManager.props(context), "cassandra-manager")
  val cluster = context.getClient
}

object Cassandra extends ExtensionId[CassandraExtension] with ExtensionIdProvider {

  override def lookup() = Cassandra
  override def createExtension(system: ExtendedActorSystem) = new CassandraExtension(system)

  def manager(implicit system: ActorSystem): ActorRef = super.get(system).manager
  def cluster(implicit system: ActorSystem): Cluster = super.get(system).cluster
}
