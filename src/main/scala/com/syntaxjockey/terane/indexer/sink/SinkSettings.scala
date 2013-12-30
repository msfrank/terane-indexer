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
import com.syntaxjockey.terane.indexer.http.JsonProtocol._

trait SinkSettings {
  val name: String
}

object SinkSettings extends DefaultJsonProtocol {
  import CassandraSinkSettings._

  def parse(name: String, config: Config): SinkSettings = {
    if (!config.hasPath("sinkType")) throw IndexerConfigException("missing required parameter 'sinkType'")
    config.getString("sinkType") match {
      case "cassandra" =>
        CassandraSinkSettings.parse(name, config)
      case unknown =>
        throw IndexerConfigException("unknown sinkType '%s'".format(unknown))
    }
  }

  implicit object SinkSettingsFormat extends RootJsonFormat[SinkSettings] {
    def write(settings: SinkSettings) = settings match {
      case settings: CassandraSinkSettings =>
        JsObject("sinkType" -> JsString("cassandra"), "settings" -> CassandraSinkSettingsFormat.write(settings))
      case unknown => throw new SerializationException("don't know how to serialize %s".format(unknown))
    }
    def read(value: JsValue) = value match {
      case obj: JsObject =>
        obj.fields.get("sinkType") match {
          case Some(sinkType) =>
            sinkType match {
              case JsString("cassandra") =>
                CassandraSinkSettingsFormat.read(obj.fields("settings"))
              case unknown =>
                throw new DeserializationException("unknown sinkType '%s'".format(unknown))
            }
          case None =>
            throw new DeserializationException("SinkSettings is missing sinkType")
        }

      case _ => throw new DeserializationException("expected SinkSettings")
    }
  }
}

case class CassandraSinkSettings(name: String, flushInterval: FiniteDuration) extends SinkSettings

object CassandraSinkSettings extends DefaultJsonProtocol {

  def parse(name: String, config: Config): CassandraSinkSettings = {
    val flushInterval = if (config.hasPath("flushInterval"))
      FiniteDuration(config.getMilliseconds("flushInterval"), TimeUnit.MILLISECONDS)
    else FiniteDuration(60, TimeUnit.SECONDS)
    new CassandraSinkSettings(name, flushInterval)
  }

  implicit val CassandraSinkSettingsFormat = jsonFormat2(CassandraSinkSettings.apply)
}

