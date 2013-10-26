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
import scala.collection.JavaConversions._
import java.nio.charset.Charset

class ZookeeperExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[ZookeeperExtension])

  val config = system.settings.config.getConfig("terane.zookeeper")

  /* configure zookeeper */
  private val retryPolicy = new ExponentialBackoffRetry(
    config.getMilliseconds("retry-sleep-time").toInt,
    config.getInt("retry-count"))
  log.debug("retryPolicy = {}", retryPolicy)

  private val connectionString = config.getStringList("servers").mkString(",")
  log.debug("connectionString = {}", connectionString)

  private val namespace = config.getString("namespace")
  log.debug("namespace = {}", namespace)

  val client = {
    val _client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy)
    log.info("connecting to zookeeper servers {}", connectionString)
    _client.start()
    if (config.hasPath("namespace"))
      _client.usingNamespace(config.getString("namespace"))
    else _client
  }

  /* create zookeeper manager */
  val manager = system.actorOf(ZookeeperManager.props(client), "zookeeper-manager")
  private val listener = new ZookeeperListener(manager)
  client.getConnectionStateListenable.addListener(listener)
  client.getUnhandledErrorListenable.addListener(listener)
}

object Zookeeper extends ExtensionId[ZookeeperExtension] with ExtensionIdProvider {

  val UTF_8_CHARSET = Charset.forName("UTF-8")

  override def lookup() = Zookeeper
  override def createExtension(system: ExtendedActorSystem) = new ZookeeperExtension(system)

  def client(implicit system: ActorSystem): CuratorFramework = super.get(system).client
}