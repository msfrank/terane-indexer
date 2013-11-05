package com.syntaxjockey.terane.indexer.zookeeper

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit

class ZookeeperSettings(val servers: Seq[String], val namespace: Option[String], val retryCount: Int, val retrySleepTime: FiniteDuration)

object ZookeeperSettings {
  def parse(config: Config): ZookeeperSettings = {
    val servers = config.getStringList("servers")
    val namespace = if (config.hasPath("namespace")) Some(config.getString("namespace")) else None
    val retryCount = config.getInt("retry-count")
    val retrySleepTime = FiniteDuration(config.getMilliseconds("retry-sleep-time"), TimeUnit.MILLISECONDS)
    new ZookeeperSettings(servers, namespace, retryCount, retrySleepTime)
  }
}
