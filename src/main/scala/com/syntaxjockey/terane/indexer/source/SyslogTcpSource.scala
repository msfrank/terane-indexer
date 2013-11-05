package com.syntaxjockey.terane.indexer.source

import akka.actor._
import akka.io._
import akka.io.TcpPipelineHandler.Init
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import java.security.KeyStore
import java.io.FileInputStream

import com.syntaxjockey.terane.indexer.source.SyslogPipelineHandler.SyslogInit
import com.syntaxjockey.terane.indexer.EventRouter.StoreEvent

/**
 * Actor implementing the syslog protocol over TCP in accordance with RFC6587:
 * http://tools.ietf.org/html/rfc6587
 *
 * if "enable-tls" is true, then the actor implements TLS for transport security
 * in accordance with RFC5425: http://tools.ietf.org/html/rfc5425
 */
class SyslogTcpSource(settings: SyslogTcpSourceSettings, eventRouter: ActorRef) extends Actor with ActorLogging {
  import akka.io.Tcp._
  import akka.io.{TcpReadWriteAdapter, BackpressureBuffer}
  import context.system

  // state
  var connections: Set[ActorRef] = Set.empty

  // if tls is enabled, then create an SSLContext
  val sslContext: Option[SSLContext] = settings.tlsSettings match {
    case Some(tlsSettings) =>
      val keystore = KeyStore.getInstance("JKS")
      keystore.load(new FileInputStream(tlsSettings.tlsKeystore), tlsSettings.tlsKeystorePassword.toCharArray)
      val truststore = KeyStore.getInstance("JKS")
      truststore.load(new FileInputStream(tlsSettings.tlsTruststore), tlsSettings.tlsTruststorePassword.toCharArray)
      val keymanagerFactory = KeyManagerFactory.getInstance("SunX509")
      keymanagerFactory.init(keystore, tlsSettings.tlsKeymanagerPassword.toCharArray)
      val trustmanagerFactory = TrustManagerFactory.getInstance("SunX509")
      trustmanagerFactory.init(truststore)
      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(keymanagerFactory.getKeyManagers, trustmanagerFactory.getTrustManagers, null)
      log.debug("enabling TLS for source")
      Some(sslContext)
    case None =>
      None
  }

  // start the bind process
  val localAddr = new InetSocketAddress(settings.interface, settings.port)
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
      settings.maxConnections match {
        case Some(_maxConnections) if connections.size == _maxConnections =>
          val handler = context.actorOf(ImmediateCloseHandler.props(connection))
          connection ! Tcp.Register(handler)
        case _ =>
          val stages = sslContext match {
            case Some(ctx) =>
              val sslEngine = ctx.createSSLEngine()
              sslEngine.setUseClientMode(false)
              settings.tlsSettings.get.tlsClientAuth match {
                case TlsClientAuth.REQUIRED => sslEngine.setNeedClientAuth(true)
                case TlsClientAuth.REQUESTED => sslEngine.setWantClientAuth(true)
                case TlsClientAuth.IGNORED =>
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
          val init = SyslogPipelineHandler.init(log, stages, settings.maxMessageSize)
          val handler = context.actorOf(TcpConnectionHandler.props(init, connection, eventRouter, settings.useSink, settings.idleTimeout))
          val pipeline = context.actorOf(TcpPipelineHandler.props(init, connection, handler))
          connection ! Tcp.Register(pipeline)   // FIXME: enable keepOpenOnPeerClosed?
          context.watch(handler)
          connections = connections + handler
          log.debug("connection from {} is registered to handler {}", remote, handler)
      }

    case Terminated(handler) =>
      connections = connections - handler
      log.debug("handler {} is unregistered", handler)
  }
}

object SyslogTcpSource {
  def props(settings: SyslogTcpSourceSettings, eventRouter: ActorRef) = Props(classOf[SyslogTcpSource], settings, eventRouter)
}

/**
 * Accept the TCP connection, then immediately close with a RST.
 */
class ImmediateCloseHandler(connection: ActorRef) extends Actor with ActorLogging {
  import Tcp.{Abort, ConnectionClosed}
  connection ! Abort
  def receive = {
    case _: ConnectionClosed =>
      log.debug("connection is closed")
      context.stop(self)
    case _ =>
  }
}

object ImmediateCloseHandler {
  def props(connection: ActorRef) = Props(classOf[ImmediateCloseHandler], connection)
}

/**
 * actor responsible for running data through the pipeline specified in SyslogInit.
 */
class SyslogPipelineHandler(init: SyslogInit, conn: ActorRef, handler: ActorRef) extends TcpPipelineHandler[SyslogContext,SyslogEvent,SyslogEvent](init, conn, handler)

object SyslogPipelineHandler {

  type SyslogInit = Init[SyslogContext, SyslogEvent, SyslogEvent]
  type SyslogPipelineStage = PipelineStage[SyslogContext, SyslogEvent, Tcp.Command, SyslogEvent, Tcp.Event]

  def init(log: LoggingAdapter, stages: SyslogPipelineStage, maxMessageSize: Option[Long]): SyslogInit = {
    new SyslogInit(stages) {
      override def makeContext(ctx: ActorContext): SyslogContext = new SyslogContext(log, ctx, maxMessageSize)
    }
  }
}

/**
 * actor responsible for managing events from a single TCP connection.
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
