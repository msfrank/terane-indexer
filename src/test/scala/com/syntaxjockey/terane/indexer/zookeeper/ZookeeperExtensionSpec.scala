package com.syntaxjockey.terane.indexer.zookeeper

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import com.syntaxjockey.terane.indexer.{TestCluster, RequiresTestCluster}

class ZookeeperExtensionSpec extends TestCluster("ZookeeperExtensionSpec") with WordSpec with MustMatchers {

  "A ZookeeperExtension" must {

    "connect to zookeeper" taggedAs RequiresTestCluster in {

    }
  }
}
