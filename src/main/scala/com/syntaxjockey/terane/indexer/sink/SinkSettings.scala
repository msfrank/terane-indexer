package com.syntaxjockey.terane.indexer.sink

import com.typesafe.config.Config
import com.syntaxjockey.terane.indexer.IndexerConfigException
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

abstract class SinkSettings

class CassandraSinkSettings(val flushInterval: Duration) extends SinkSettings

object CassandraSinkSettings {
  def parse(config: Config): CassandraSinkSettings = {
    val flushInterval = Duration(config.getMilliseconds("flush-interval"), TimeUnit.MILLISECONDS)
    new CassandraSinkSettings(flushInterval)
  }
}

object SinkSettings {
  def parse(config: Config): SinkSettings = {
    if (!config.hasPath("sink-type")) throw new IndexerConfigException("missing required parameter 'sink-type'")
    config.getString("sink-type") match {
      case "cassandra" =>
        CassandraSinkSettings.parse(config)
      case unknown =>
        throw new IndexerConfigException("unknown sink-type '%s'".format(unknown))
    }
  }
}
