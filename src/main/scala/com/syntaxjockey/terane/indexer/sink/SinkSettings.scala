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

object SinkSettings extends DefaultJsonProtocol {
  import CassandraSinkSettings._

  def parse(name: String, config: Config): SinkSettings = {
    if (!config.hasPath("sink-type")) throw IndexerConfigException("missing required parameter 'sink-type'")
    config.getString("sink-type") match {
      case "cassandra" =>
        CassandraSinkSettings.parse(name, config)
      case unknown =>
        throw IndexerConfigException("unknown sink-type '%s'".format(unknown))
    }
  }

  implicit object SinkSettingsFormat extends RootJsonFormat[SinkSettings] {
    def write(settings: SinkSettings) = settings match {
      case cassandraSinkSettings: CassandraSinkSettings =>
        cassandraSinkSettings.toJson
      case unknown => throw new SerializationException("don't know how to serialize %s".format(unknown))
    }
    def read(value: JsValue) = value match {
      case obj: JsObject =>
        obj.fields.get("sink-type") match {
          case Some(sinkType) =>
            sinkType match {
              case JsString("cassandra") =>
                CassandraSinkSettingsFormat.read(value)
              case unknown =>
                throw new DeserializationException("unknown sink-type '%s'".format(unknown))
            }
          case None =>
            throw new DeserializationException("SinkSettings is missing sink-type")
        }

      case _ => throw new DeserializationException("expected SinkSettings")
    }
  }
}
