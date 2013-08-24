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

import com.typesafe.config.Config
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.CuratorFrameworkFactory
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import scala.collection.JavaConversions._

class ZookeeperClient(config: Config) {

  private val log = LoggerFactory.getLogger(classOf[ZookeeperClient])

  /* connect to zookeeper */
  val retryPolicy = new ExponentialBackoffRetry(
    config.getMilliseconds("retry-sleep-time").toInt,
    config.getInt("retry-count"))
  log.debug("retryPolicy = {}", retryPolicy)

  val connectionString = config.getStringList("servers").mkString(",")
  log.debug("connectionString = {}", connectionString)

  val client = {
    val _client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy)
    log.info("connecting to zookeeper servers {}", connectionString)
    _client.start()
    _client.usingNamespace(config.getString("namespace"))
  }
  log.debug("client = {}", client)

  def close() {
    client.close()
  }
}

object ZookeeperClient {
  val UTF_8_CHARSET = Charset.forName("UTF-8")
}
