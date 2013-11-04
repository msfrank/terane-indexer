package com.syntaxjockey.terane.indexer.source

import com.typesafe.config.Config
import scala.concurrent.duration.Duration
import com.syntaxjockey.terane.indexer.IndexerConfigException
import java.util.concurrent.TimeUnit

abstract class SourceSettings(val useSink: String, val allowSinkCreation: Boolean, val allowSinkRouting: Boolean)

abstract class SyslogSourceSettings(
  val interface: String,
  val port: Int,
  override val useSink: String,
  override val allowSinkCreation: Boolean,
  override val allowSinkRouting: Boolean)
  extends SourceSettings(useSink, allowSinkCreation, allowSinkRouting)

class SyslogUdpSourceSettings(
  override val interface: String,
  override val port: Int,
  override val useSink: String,
  override val allowSinkCreation: Boolean,
  override val allowSinkRouting: Boolean)
  extends SyslogSourceSettings(interface, port, useSink, allowSinkCreation, allowSinkRouting)

object SyslogUdpSourceSettings {
  def parse(config: Config): SyslogUdpSourceSettings = {
    val interface = config.getString("interface")
    val port = config.getInt("port")
    val defaultSink = config.getString("use-sink")
    val allowSinkCreation = config.getBoolean("allow-sink-creation")
    val allowSinkRouting = config.getBoolean("allow-sink-routing")
    new SyslogUdpSourceSettings(interface, port, defaultSink, allowSinkCreation, allowSinkRouting)
  }
}

object TlsClientAuth extends Enumeration {
  type TlsClientAuth = Value
  val REQUIRED = Value("required")
  val REQUESTED = Value("requested")
  val IGNORED = Value("ignored")
}

class SyslogTcpTlsSettings(
  val tlsClientAuth: TlsClientAuth.TlsClientAuth,
  val tlsKeystore: String,
  val tlsTruststore: String,
  val tlsKeystorePassword: String,
  val tlsTruststorePassword: String,
  val tlsKeymanagerPassword: String)

object SyslogTcpTlsSettings {
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
        case unknown => throw new IndexerConfigException("unknown tls-client auth mode '%s'".format(unknown))
      }
    } else TlsClientAuth.REQUESTED
    new SyslogTcpTlsSettings(tlsClientAuth, tlsKeystore, tlsTruststore, tlsKeystorePassword, tlsTruststorePassword, tlsKeymanagerPassword)
  }
}

class SyslogTcpSourceSettings(
  override val interface: String,
  override val port: Int,
  val idleTimeout: Option[Duration],
  val maxConnections: Option[Int],
  val maxMessageSize: Option[Long],
  val tlsSettings: Option[SyslogTcpTlsSettings],
  override val useSink: String,
  override val allowSinkCreation: Boolean,
  override val allowSinkRouting: Boolean)
  extends SyslogSourceSettings(interface, port, useSink, allowSinkCreation, allowSinkRouting)

object SyslogTcpSourceSettings {
  def parse(config: Config): SyslogTcpSourceSettings = {
    val interface = config.getString("interface")
    val port = config.getInt("port")
    val idleTimeout = if (config.hasPath("idle-timeout")) Some(Duration(config.getMilliseconds("idle-timeout"), TimeUnit.MILLISECONDS)) else None
    val maxMessageSize = if (config.hasPath("max-message-size")) Some(config.getBytes("max-message-size").toLong) else None
    val maxConnections = if (config.hasPath("max-connections")) Some(config.getInt("max-connections")) else None
    val defaultSink = config.getString("use-sink")
    val allowSinkCreation = config.getBoolean("allow-sink-creation")
    val allowSinkRouting = config.getBoolean("allow-sink-routing")
    val tlsSettings = if (config.hasPath("enable-tls") && config.getBoolean("enable-tls")) Some(SyslogTcpTlsSettings.parse(config)) else None
    new SyslogTcpSourceSettings(interface, port, idleTimeout, maxConnections, maxMessageSize, tlsSettings, defaultSink, allowSinkCreation, allowSinkRouting)
  }
}

object SourceSettings {
  def parse(config: Config): SourceSettings = {
    if (!config.hasPath("source-type")) throw new IndexerConfigException("missing required parameter 'source-type'")
    config.getString("source-type") match {
      case "syslog-udp" =>
        SyslogUdpSourceSettings.parse(config)
      case "syslog-tcp" =>
        SyslogTcpSourceSettings.parse(config)
      case unknown =>
        throw new IndexerConfigException("unknown source-type '%s'".format(unknown))
    }
  }
}


