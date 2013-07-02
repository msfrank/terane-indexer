package com.syntaxjockey.terane.indexer.sink

import org.slf4j.LoggerFactory
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{Keyspace, AstyanaxContext}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.typesafe.config.Config
import scala.collection.JavaConversions._

class CassandraClient(config: Config) {

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
   * get a Keyspace from the client connection.
   *
   * @param keyspaceName
   * @return
   */
  def getKeyspace(keyspaceName: String): Keyspace = cluster.getKeyspace(keyspaceName)

  /**
   * close the client connection.
   */
  def close() {
    context.shutdown()
  }
}
