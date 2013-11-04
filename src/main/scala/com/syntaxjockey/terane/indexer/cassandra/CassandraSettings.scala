package com.syntaxjockey.terane.indexer.cassandra

import com.typesafe.config.Config
import scala.collection.JavaConversions._

class CassandraSettings(val servers: Seq[String], val port: Int, poolName: String, maxConnsPerHost: Int, clusterName: String)

object CassandraSettings {
  def parse(config: Config): CassandraSettings = {
    val servers = config.getStringList("servers")
    val port = config.getInt("port")
    val poolName = config.getString("connection-pool-name")
    val maxConnsPerHost = config.getInt("max-conns-per-host")
    val clusterName = config.getString("cluster-name")
    new CassandraSettings(servers, port, poolName, maxConnsPerHost, clusterName)
  }
}
