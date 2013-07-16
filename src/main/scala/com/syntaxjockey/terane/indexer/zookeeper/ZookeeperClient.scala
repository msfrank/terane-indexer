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
