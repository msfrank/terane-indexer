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

import org.slf4j.LoggerFactory
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{Keyspace, AstyanaxContext}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.typesafe.config.Config
import scala.collection.JavaConversions._

class CassandraClient(config: Config) extends CassandraKeyspaceOperations {

  private val log = LoggerFactory.getLogger(classOf[CassandraClient])

  val configuration = new AstyanaxConfigurationImpl()
    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
  val seeds = config.getStringList("seeds").mkString(",")
  val poolConfiguration = new ConnectionPoolConfigurationImpl(config.getString("connection-pool-name"))
    .setPort(config.getInt("port"))
    .setMaxConnsPerHost(config.getInt("max-conns-per-host"))
    .setSeeds(seeds)
  val connectionPoolMonitor = new CountingConnectionPoolMonitor()
  val clusterName = config.getString("cluster-name")
  val context = new AstyanaxContext.Builder()
    .forCluster(clusterName)
    .withAstyanaxConfiguration(configuration)
    .withConnectionPoolConfiguration(poolConfiguration)
    .withConnectionPoolMonitor(connectionPoolMonitor)
    .buildCluster(ThriftFamilyFactory.getInstance())
  log.info("connecting to cluster {}", clusterName)
  context.start()
  val cluster = context.getClient

  /**
   * close the client connection.
   */
  def close() {
    context.shutdown()
  }
}
