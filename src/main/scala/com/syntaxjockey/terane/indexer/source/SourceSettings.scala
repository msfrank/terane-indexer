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

package com.syntaxjockey.terane.indexer.source

import com.typesafe.config.Config
import spray.json._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.IndexerConfigException
import com.syntaxjockey.terane.indexer.http.JsonProtocol._

trait SourceSettings {
  val name: String
}

object SourceSettings {
  import SyslogUdpSourceSettings.SyslogUdpSourceSettingsFormat
  import SyslogTcpSourceSettings.SyslogTcpSourceSettingsFormat

  def parse(config: Config): SourceSettings = {
    if (!config.hasPath("sourceType")) throw IndexerConfigException("missing required parameter 'sourceType'")
    config.getString("sourceType") match {
      case "syslog-udp" =>
        SyslogUdpSourceSettings.parse(config)
      case "syslog-tcp" =>
        SyslogTcpSourceSettings.parse(config)
      case unknown =>
        throw IndexerConfigException("unknown sourceType '%s'".format(unknown))
    }
  }

  implicit object SourceSettingsFormat extends JsonFormat[SourceSettings] {
    def write(settings: SourceSettings) = settings match {
      case settings: SyslogUdpSourceSettings =>
        SyslogUdpSourceSettingsFormat.write(settings) match {
          case JsObject(fields) =>
            JsObject(fields + ("sourceType" -> JsString("syslog-udp")))
          case unknown =>
            throw new SerializationException("source settings are malformed: %s".format(unknown))
        }
      case settings: SyslogTcpSourceSettings =>
        SyslogTcpSourceSettingsFormat.write(settings) match {
          case JsObject(fields) =>
            JsObject(fields + ("sourceType" -> JsString("syslog-tcp")))
          case unknown =>
            throw new SerializationException("source settings are malformed: %s".format(unknown))
        }
      case unknown => throw new SerializationException("don't know how to serialize %s".format(unknown))
    }

    def read(value: JsValue) = value match {
      case obj: JsObject =>
        obj.fields.get("sourceType") match {
          case Some(sourceType) =>
            val sourceSettings = JsObject(obj.fields - "sourceType")
            sourceType match {
              case JsString("syslog-udp") =>
                SyslogUdpSourceSettingsFormat.read(sourceSettings)
              case JsString("syslog-tcp") =>
                SyslogTcpSourceSettingsFormat.read(sourceSettings)
              case unknown =>
                throw new DeserializationException("unknown sourceType '%s'".format(unknown))
            }
          case None =>
            throw new DeserializationException("SourceSettings is missing sourceType")
        }

      case _ => throw new DeserializationException("expected SourceSettings")
    }
  }
}

trait SyslogSourceSettings extends SourceSettings {
  val interface: String
  val port: Int
}

/**
 *
 */
object TlsClientAuth extends Enumeration with DefaultJsonProtocol {
  type TlsClientAuth = Value
  val REQUIRED = Value("required")
  val REQUESTED = Value("requested")
  val IGNORED = Value("ignored")

  implicit object TlsClientAuthFormat extends RootJsonFormat[TlsClientAuth] {
    def write(obj: TlsClientAuth): JsValue = JsString(obj.toString)
    def read(value: JsValue): TlsClientAuth = value match {
      case JsString(str) => TlsClientAuth.withName(str)
      case _ => throw new DeserializationException("expected TlsClientAuth")
    }
  }
}

/**
 *
 */
case class SyslogTcpTlsSettings(
  tlsClientAuth: TlsClientAuth.TlsClientAuth,
  tlsKeystore: String,
  tlsTruststore: String,
  tlsKeystorePassword: String,
  tlsTruststorePassword: String,
  tlsKeymanagerPassword: String)

object SyslogTcpTlsSettings extends DefaultJsonProtocol {

  def parse(config: Config): SyslogTcpTlsSettings = {
    val tlsKeystore = config.getString("tls-keystore")
    val tlsTruststore = config.getString("tls-truststore")
    val tlsPassword = if (config.hasPath("tls-password")) config.getString("tls-password") else null
    val tlsKeystorePassword = if (config.hasPath("tls-keystore-password")) config.getString("tls-keystore-password") else tlsPassword
    val tlsTruststorePassword = if (config.hasPath("tls-truststore-password")) config.getString("tls-truststore-password") else tlsPassword
    val tlsKeymanagerPassword = if (config.hasPath("tls-keymanager-password")) config.getString("tls-keymanager-password") else tlsPassword
    val tlsClientAuth = if (config.hasPath("tls-client-auth")) {
      config.getString("tls-client-auth").toLowerCase match {
        case "required" => TlsClientAuth.REQUIRED
        case "requested" => TlsClientAuth.REQUESTED
        case "ignored" => TlsClientAuth.IGNORED
        case unknown => throw IndexerConfigException("unknown tls-client auth mode '%s'".format(unknown))
      }
    } else TlsClientAuth.REQUESTED
    new SyslogTcpTlsSettings(tlsClientAuth, tlsKeystore, tlsTruststore, tlsKeystorePassword, tlsTruststorePassword, tlsKeymanagerPassword)
  }

  implicit val SyslogTcpTlsFormat = jsonFormat6(SyslogTcpTlsSettings.apply)
}

/**
 *
 */
case class SyslogTcpSourceSettings(
  name: String,
  interface: String,
  port: Int,
  idleTimeout: Option[FiniteDuration],
  maxConnections: Option[Int],
  maxMessageSize: Option[Long],
  tlsSettings: Option[SyslogTcpTlsSettings]) extends SyslogSourceSettings

object SyslogTcpSourceSettings extends DefaultJsonProtocol {

  def parse(config: Config): SyslogTcpSourceSettings = {
    val name = config.getString("name")
    val interface = config.getString("interface")
    val port = config.getInt("port")
    val idleTimeout = if (config.hasPath("idle-timeout")) Some(FiniteDuration(config.getMilliseconds("idle-timeout"), TimeUnit.MILLISECONDS)) else None
    val maxMessageSize = if (config.hasPath("max-message-size")) Some(config.getBytes("max-message-size").toLong) else None
    val maxConnections = if (config.hasPath("max-connections")) Some(config.getInt("max-connections")) else None
    val tlsSettings = if (config.hasPath("enable-tls") && config.getBoolean("enable-tls")) Some(SyslogTcpTlsSettings.parse(config)) else None
    new SyslogTcpSourceSettings(name, interface, port, idleTimeout, maxConnections, maxMessageSize, tlsSettings)
  }

  implicit val SyslogTcpSourceSettingsFormat = jsonFormat7(SyslogTcpSourceSettings.apply)
}

/**
 *
 */
case class SyslogUdpSourceSettings(name: String, interface: String, port: Int) extends SyslogSourceSettings

object SyslogUdpSourceSettings extends DefaultJsonProtocol {

  def parse(config: Config): SyslogUdpSourceSettings = {
    val name = config.getString("name")
    val interface = config.getString("interface")
    val port = config.getInt("port")
    new SyslogUdpSourceSettings(name, interface, port)
  }

  implicit val SyslogUdpSourceSettingsFormat = jsonFormat3(SyslogUdpSourceSettings.apply)
}