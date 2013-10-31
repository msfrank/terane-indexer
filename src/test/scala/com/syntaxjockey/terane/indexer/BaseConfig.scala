package com.syntaxjockey.terane.indexer

import com.typesafe.config.{Config, ConfigFactory}

trait ComposableConfig {
  def config: Config
  def ++(other: MergedConfig): MergedConfig = new MergedConfig(other.config.withFallback(config))
  def ++(other: ComposableConfig): MergedConfig = new MergedConfig(other.config.withFallback(config))
  def +(other: MergedConfig): Config = other.config.withFallback(config)
  def +(other: ComposableConfig): Config = other.config.withFallback(config)
  def +(other: Config): Config = other.withFallback(config)
}

class MergedConfig(val config: Config) extends ComposableConfig

object BaseConfig extends ComposableConfig {
  val config = ConfigFactory.parseString("terane {}")
}

object AkkaConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = DEBUG
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  actor {
      |    debug {
      |      receive = on
      |      autoreceive = on
      |      lifecycle = on
      |      fsm = on
      |      event-stream = on
      |      unhandled = on
      |      router-misconfiguration = on
      |    }
      |  }
      |}
    """.stripMargin)
}

object AkkaClusterConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |akka {
      |  actor {
      |    provider = "akka.cluster.ClusterActorRefProvider"
      |    serializers { }
      |    serialization-bindings { }
      |  }
      |  remote {
      |    enabled-transports = ["akka.remote.netty.tcp"]
      |  }
      |}
    """.stripMargin)
}

object SprayConfig extends ComposableConfig {
  val config = ConfigFactory.parseString(
    """
      |spray {
      |  can {
      |    server {
      |      idle-timeout = 60 s
      |    }
      |  }
      |}
    """.stripMargin)
}

object ConfigConversions {
  implicit def composableConfig2Config(composable: ComposableConfig): Config = composable.config
  implicit def mergedConfig2Config(merged: MergedConfig): Config = merged.config
}