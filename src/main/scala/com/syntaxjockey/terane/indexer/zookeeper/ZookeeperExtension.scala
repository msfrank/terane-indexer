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

package com.syntaxjockey.terane.indexer.zookeeper

import akka.actor._
import org.slf4j.LoggerFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import com.netflix.curator.framework.api.UnhandledErrorListener
import scala.collection.JavaConversions._
import java.nio.charset.Charset

class ZookeeperListener extends ConnectionStateListener with UnhandledErrorListener {

  val log = LoggerFactory.getLogger(classOf[ZookeeperListener])

  def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    log.debug("zookeeper state changed to {}", newState.name())
  }

  def unhandledError(message: String, reason: Throwable) {
    log.error("caught unhandled error from zookeeper: {}", message)
  }
}

class ZookeeperManager(client: CuratorFramework) extends Actor with ActorLogging {

  val listener = new ZookeeperListener()
  client.getConnectionStateListenable.addListener(listener)
  client.getUnhandledErrorListenable.addListener(listener)

  log.debug("started zookeeper manager")

  def receive = {
    case _ =>
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.warning("restarted zookeeper manager")
  }

  override def postStop() {
    client.close()
    log.debug("stopped zookeeper manager")
  }
}

object ZookeeperManager {
  def props(client: CuratorFramework) = Props(classOf[ZookeeperManager], client)
}

class ZookeeperExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[ZookeeperExtension])

  val config = system.settings.config.getConfig("terane.zookeeper")

  /* configure zookeeper */
  val retryPolicy = new ExponentialBackoffRetry(
    config.getMilliseconds("retry-sleep-time").toInt,
    config.getInt("retry-count"))
  log.debug("retryPolicy = {}", retryPolicy)

  val connectionString = config.getStringList("servers").mkString(",")
  log.debug("connectionString = {}", connectionString)

  val namespace = config.getString("namespace")
  log.debug("namespace = {}", namespace)

  val client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy)

  /* create zookeeper manager */
  val manager = system.actorOf(ZookeeperManager.props(client), "zookeeper-manager")

  /* start zookeeper */
  log.info("connecting to zookeeper servers {}", connectionString)
  client.start()

  /* root ourselves in the specified namespace */
  client.usingNamespace(config.getString("namespace"))
}

object Zookeeper extends ExtensionId[ZookeeperExtension] with ExtensionIdProvider {

  val UTF_8_CHARSET = Charset.forName("UTF-8")

  override def lookup() = Zookeeper
  override def createExtension(system: ExtendedActorSystem) = new ZookeeperExtension(system)

  def manager(implicit system: ActorSystem): ActorRef = super.get(system).manager
  def client(implicit system: ActorSystem): CuratorFramework = super.get(system).client
}