package com.syntaxjockey.terane.indexer

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import com.typesafe.config.{ConfigValueFactory, ConfigFactory}

class IndexerConfigExtensionSpec extends TestCluster("IndexerConfigExtensionSpec") with WordSpec with MustMatchers {

  val baseConfig = ConfigFactory.parseString(
    """
      |terane {
      |  config {
      |    file = [ "conf/terane-test1.conf", "conf/terane-test2.conf" ]
      |  }
      |  zookeeper {
      |  }
      |  cassandra {
      |  }
      |  sources {
      |  }
      |  sinks {
      |  }
      |  queries {
      |  }
      |}
      |akka {
      |  loglevel = DEBUG
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  actor {
      |    provider = "akka.cluster.ClusterActorRefProvider"
      |    serializers { }
      |    serialization-bindings { }
      |    debug {
      |      receive = on
      |      lifecycle = on
      |      fsm = on
      |      event-stream = on
      |      unhandled = on
      |      router-misconfiguration = on
      |    }
      |  }
      |  remote {
      |    enabled-transports = ["akka.remote.netty.tcp"]
      |    netty {
      |      tcp {
      |        port = 0
      |      }
      |    }
      |  }
      |  cluster {
      |    log-info = off
      |  }
      |}
      |spray {
      |  can {
      |    server {
      |      idle-timeout = 60 s
      |    }
      |  }
      |}
    """.stripMargin)

  "IndexerConfigExtension" must {

    "find the most preferred terane.conf from a list of candidate files" in {
      val config = IndexerConfig.loadConfigFile(baseConfig)
      config.hasPath("terane.sources.localhost-udp") must be(true)
    }

    "find the second most preferred terane.conf from a list of candidate files when the first file is not present" in {
      import scala.collection.JavaConversions._
      val candidates = Seq("conf/terane-missing.conf", "conf/terane-test2.conf")
      val config = IndexerConfig.loadConfigFile(baseConfig.withValue("terane.config.file", ConfigValueFactory.fromIterable(candidates)))
      config.hasPath("terane.sources.localhost-tcp") must be(true)
    }

    "find the terane.conf specified by system property -Dterane.config.file" in {
      val oldProp = System.getProperty("terane.config.file")
      System.setProperty("terane.config.file", "conf/terane-test3.conf")
      try {
        ConfigFactory.invalidateCaches()
        val config = IndexerConfig.loadConfigFile(ConfigFactory.defaultOverrides().withFallback(baseConfig))
        println(config)
        config.hasPath("terane.sources.localhost-tcptls") must be(true)
      } finally {
        oldProp match {
          case null =>
            System.clearProperty("terane.config.file")
          case value =>
            System.setProperty("terane.config.file", value)
        }
        ConfigFactory.invalidateCaches()
      }
    }
  }
}
