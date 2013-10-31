package com.syntaxjockey.terane.indexer.zookeeper

import com.typesafe.config.ConfigFactory

import com.syntaxjockey.terane.indexer.ComposableConfig

object ZookeeperConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |terane {
      |  zookeeper {
      |    servers = [ "127.0.0.1:2181" ]
      |    namespace = "terane"
      |    retry-sleep-time = 1 second
      |    retry-count = 5
      |  }
      |}
    """.stripMargin)
}
