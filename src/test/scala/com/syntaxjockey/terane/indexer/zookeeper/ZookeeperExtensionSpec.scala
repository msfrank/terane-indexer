package com.syntaxjockey.terane.indexer.zookeeper

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import com.syntaxjockey.terane.indexer.RequiresTestCluster

class ZookeeperExtensionSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ZookeeperExtensionSpec"))

  "A ZookeeperExtension" must {

    "connect to zookeeper" taggedAs RequiresTestCluster in {

    }
  }
}
