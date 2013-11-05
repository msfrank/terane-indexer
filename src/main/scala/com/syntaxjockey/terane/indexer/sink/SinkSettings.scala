package com.syntaxjockey.terane.indexer.sink

import com.typesafe.config.Config
import com.syntaxjockey.terane.indexer.IndexerConfigException
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit

abstract class SinkSettings

class CassandraSinkSettings(val flushInterval: Option[FiniteDuration]) extends SinkSettings

object CassandraSinkSettings {
  def parse(config: Config): CassandraSinkSettings = {
    val flushInterval = if (config.hasPath("flush-interval")) Some(FiniteDuration(config.getMilliseconds("flush-interval"), TimeUnit.MILLISECONDS)) else None
    new CassandraSinkSettings(flushInterval)
  }
}

object SinkSettings {
  def parse(config: Config): SinkSettings = {
    if (!config.hasPath("sink-type")) throw IndexerConfigException("missing required parameter 'sink-type'")
    config.getString("sink-type") match {
      case "cassandra" =>
        CassandraSinkSettings.parse(config)
      case unknown =>
        throw IndexerConfigException("unknown sink-type '%s'".format(unknown))
    }
  }
}
