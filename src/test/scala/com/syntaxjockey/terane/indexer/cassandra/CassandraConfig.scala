package com.syntaxjockey.terane.indexer.cassandra

import com.typesafe.config.ConfigFactory

import com.syntaxjockey.terane.indexer.ComposableConfig

object CassandraConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
    |terane {
    |  cassandra {
    |    connection-pool-name = "Default Connection Pool"
    |    port = 9160
    |    max-conns-per-host = 1
    |    seeds = [ "127.0.0.1:9160" ]
    |    cluster-name = "Default Cluster"
    |  }
    |}
    """.stripMargin)
}
