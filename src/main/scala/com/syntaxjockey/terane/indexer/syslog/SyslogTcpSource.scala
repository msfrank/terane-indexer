package com.syntaxjockey.terane.indexer.syslog

import akka.actor._
import akka.io._
import akka.io.TcpPipelineHandler.Init
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import scala.concurrent.duration._
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext

import com.syntaxjockey.terane.indexer.syslog.SyslogPipelineHandler.SyslogInit
import com.syntaxjockey.terane.indexer.EventRouter.StoreEvent
import java.util.concurrent.TimeUnit

/**
 * Actor implementing the syslog protocol over TCP in accordance with RFC6587:
 * http://tools.ietf.org/html/rfc6587
 *
 * if "enable-tls" is true, then the actor implements TLS for transport security
 * in accordance with RFC5425: http://tools.ietf.org/html/rfc5425
 */
class SyslogTcpSource(config: Config, eventRouter: ActorRef) extends Actor with ActorLogging {
  import akka.io.Tcp._
  import akka.io.{TcpReadWriteAdapter, BackpressureBuffer}
  import context.system

  // config
  val syslogPort = config.getInt("port")
  val syslogInterface = config.getString("interface")
  val enableTls = if (config.hasPath("enable-tls")) config.getBoolean("enable-tls") else false
  val idleTimeout = if (config.hasPath("idle-timeout")) Some(Duration(config.getMilliseconds("idle-timeout"), TimeUnit.MILLISECONDS)) else None
  val defaultSink = config.getString("use-sink")
  val allowSinkRouting = config.getBoolean("allow-sink-routing")
  val allowSinkCreate = config.getBoolean("allow-sink-creation")

  val keystoreFile = if (config.hasPath("tls-keystore")) Some(config.getString("tls-keystore")) else None
  val truststoreFile = if (config.hasPath("tls-truststore")) Some(config.getString("tls-truststore")) else None
  val password = if (config.hasPath("tls-password")) config.getString("tls-password") else null
  val keystorePassword = if (config.hasPath("tls-keystore-password")) config.getString("tls-keystore-password") else password
  val truststorePassword = if (config.hasPath("tls-truststore-password")) config.getString("tls-truststore-password") else password
  val keymanagerPassword = if (config.hasPath("tls-keymanager-password")) config.getString("tls-keymanager-password") else password
  val tlsClientAuth = if (config.hasPath("tls-client-auth")) {
    config.getString("tls-client-auth") match {
      case "required" => "required"
      case "requested" => "requested"
      case "ignored" => "ignored"
      case unknown => throw new Exception("unknown tls-client auth mode '%s'".format(unknown))
    }
  } else "requested"

  // if tls is enabled, then create an SSLContext
  val sslContext: Option[SSLContext] = if (enableTls) {
    import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
    import java.security.KeyStore
    import java.io.FileInputStream
    val keystore = KeyStore.getInstance("JKS")
    keystore.load(new FileInputStream(keystoreFile.get), keystorePassword.toCharArray)
    val truststore = KeyStore.getInstance("JKS")
    truststore.load(new FileInputStream(truststoreFile.get), truststorePassword.toCharArray)
    val keymanagerFactory = KeyManagerFactory.getInstance("SunX509")
    keymanagerFactory.init(keystore, keymanagerPassword.toCharArray)
    val trustmanagerFactory = TrustManagerFactory.getInstance("SunX509")
    trustmanagerFactory.init(truststore)
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keymanagerFactory.getKeyManagers, trustmanagerFactory.getTrustManagers, null)
    log.debug("enabling TLS for source")
    Some(sslContext)
  } else None

  // start the bind process
  val localAddr = new InetSocketAddress(syslogInterface, syslogPort)
  log.debug("attempting to bind to {}", localAddr)
  akka.io.IO(Tcp) ! Bind(self, localAddr)

  def receive = {

    case Bound(_localAddr) =>
      log.debug("listening on {}", _localAddr)

    case CommandFailed(b: Bind) =>
      log.error("failed to bind to {}", b.localAddress)

    case CommandFailed(command) =>
      log.error("{} command failed", command)

    case Connected(remote, local) =>
      val connection = sender
      val stages = sslContext match {
        case Some(ctx) =>
          val sslEngine = ctx.createSSLEngine()
          sslEngine.setUseClientMode(false)
          tlsClientAuth match {
            case "required" => sslEngine.setNeedClientAuth(true)
            case "requested" => sslEngine.setWantClientAuth(true)
            case "ignored" =>
            case default => sslEngine.setWantClientAuth(true)
          }
          new ProcessFrames() >>
          new ProcessTcp() >>
          new TcpReadWriteAdapter() >>
          new SslTlsSupport(sslEngine) >>
          new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000)
        case None =>
          new ProcessFrames() >>
          new ProcessTcp() >>
          new TcpReadWriteAdapter() >>
          new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000)
      }
      val init = SyslogPipelineHandler.init(log, stages)
      val handler = context.actorOf(TcpConnectionHandler.props(init, connection, eventRouter, defaultSink, idleTimeout), "handler")
      val pipeline = context.actorOf(TcpPipelineHandler.props(init, connection, handler), "pipeline")
      connection ! Tcp.Register(pipeline)   // FIXME: enable keepOpenOnPeerClosed?
      log.debug("registered connection from {}", remote)
  }
}

object SyslogTcpSource {
  def props(config: Config, eventRouter: ActorRef) = Props(classOf[SyslogTcpSource], config, eventRouter)
}

/**
 *
 * @param init
 * @param conn
 * @param handler
 */
class SyslogPipelineHandler(init: SyslogInit, conn: ActorRef, handler: ActorRef) extends TcpPipelineHandler[SyslogContext,SyslogEvent,SyslogEvent](init, conn, handler)

object SyslogPipelineHandler {

  type SyslogInit = Init[SyslogContext, SyslogEvent, SyslogEvent]
  type SyslogPipelineStage = PipelineStage[SyslogContext, SyslogEvent, Tcp.Command, SyslogEvent, Tcp.Event]

  def init(log: LoggingAdapter, stages: SyslogPipelineStage): SyslogInit = {
    new SyslogInit(stages) {
      override def makeContext(ctx: ActorContext): SyslogContext = new SyslogContext(log, ctx)
    }
  }
}

/**
 *
 * @param init
 * @param connection
 * @param eventRouter
 * @param defaultSink
 * @param idleTimeout
 */
class TcpConnectionHandler(init: SyslogInit, connection: ActorRef, eventRouter: ActorRef, defaultSink: String, idleTimeout: Option[FiniteDuration]) extends Actor with SyslogReceiver with ActorLogging {
  import init._
  import Tcp.{ConnectionClosed, Abort, Close}
  import context.dispatcher

  var idleTimer: Option[Cancellable] = idleTimeout match {
    case Some(timeout) =>
      Some(context.system.scheduler.scheduleOnce(timeout, connection, Close))
    case None => None
  }

  def receive = {

    case Event(message: Message) =>
      log.debug("received {}", message)
      for (cancellable <- idleTimer) { cancellable.cancel() }
      eventRouter ! StoreEvent(defaultSink, message)
      idleTimer = idleTimeout match {
        case Some(timeout) =>
          Some(context.system.scheduler.scheduleOnce(timeout, connection, Close))
        case None => None
      }

    case Event(SyslogIncomplete) =>
      // ignore SyslogIncomplete messages

    case Event(failure: SyslogFailure) =>
      log.debug("failed to process TCP message: {}", failure.getCause.getMessage)
      // there is no way to notify the client of error other than simply terminating the connection
      connection ! Abort

    case _: ConnectionClosed =>
      log.debug("connection is closed")
      context.stop(self)
  }
}

object TcpConnectionHandler {
  def props(init: SyslogInit, connection: ActorRef, eventRouter: ActorRef, defaultSink: String, idleTimeout: Option[FiniteDuration]) = {
    Props(classOf[TcpConnectionHandler], init, connection, eventRouter, defaultSink, idleTimeout)
  }
}
