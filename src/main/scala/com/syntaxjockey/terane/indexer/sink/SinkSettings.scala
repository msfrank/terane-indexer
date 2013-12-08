package com.syntaxjockey.terane.indexer.sink

import com.typesafe.config.Config
import spray.json._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.IndexerConfigException

abstract class SinkSettings {
  val name: String
}

case class CassandraSinkSettings(name: String, flushInterval: FiniteDuration) extends SinkSettings

object CassandraSinkSettings extends DefaultJsonProtocol {

  def parse(name: String, config: Config): CassandraSinkSettings = {
    val flushInterval = if (config.hasPath("flush-interval"))
      FiniteDuration(config.getMilliseconds("flush-interval"), TimeUnit.MILLISECONDS)
    else FiniteDuration(60, TimeUnit.SECONDS)
    new CassandraSinkSettings(name, flushInterval)
  }

  /* convert FiniteDuration class */
  implicit object FiniteDurationFormat extends RootJsonFormat[FiniteDuration] {
    def write(duration: FiniteDuration) = JsNumber(duration.toMillis)
    def read(value: JsValue) = value match {
      case JsNumber(duration) => FiniteDuration(duration.toLong, TimeUnit.MILLISECONDS)
      case _ => throw new DeserializationException("expected FiniteDuration")
    }
  }

  implicit val CassandraSinkSettingsFormat = jsonFormat2(CassandraSinkSettings.apply)
}

object SinkSettings {
  def parse(name: String, config: Config): SinkSettings = {
    if (!config.hasPath("sink-type")) throw IndexerConfigException("missing required parameter 'sink-type'")
    config.getString("sink-type") match {
      case "cassandra" =>
        CassandraSinkSettings.parse(name, config)
      case unknown =>
        throw IndexerConfigException("unknown sink-type '%s'".format(unknown))
    }
  }
}
