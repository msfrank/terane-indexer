package com.syntaxjockey.terane.indexer.source

import akka.actor._
import akka.io._
import akka.io.TcpPipelineHandler.Init
import akka.event.LoggingAdapter
import org.apache.curator.framework.CuratorFramework
import scala.concurrent.duration._
import java.net.{URLEncoder, InetSocketAddress}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import java.security.KeyStore
import java.io.FileInputStream

import com.syntaxjockey.terane.indexer.source.SyslogPipelineHandler.SyslogInit
import com.syntaxjockey.terane.indexer.StoreEvent
import com.syntaxjockey.terane.indexer.{Instrumented, Source, SourceRef}
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.source.SourceSettings.SourceSettingsFormat

/**
 * Actor implementing the syslog protocol over TCP in accordance with RFC6587:
 * http://tools.ietf.org/html/rfc6587
 *
 * if "enable-tls" is true, then the actor implements TLS for transport security
 * in accordance with RFC5425: http://tools.ietf.org/html/rfc5425
 */
class SyslogTcpSource(name: String, settings: SyslogTcpSourceSettings, zookeeperPath: String, eventRouter: ActorRef) extends Actor
with ActorLogging with Instrumented {
  import akka.io.Tcp._
  import akka.io.{TcpReadWriteAdapter, BackpressureBuffer}
  import context.system

  // metrics
  val messagesReceived = metrics.meter("messages-received", "messages")
  val messagesDropped = metrics.meter("messages-dropped", "messages")

  // config
  val localAddr = new InetSocketAddress(settings.interface, settings.port)
  val sslContext: Option[SSLContext] = settings.tlsSettings match {
    // if tls is enabled, then create an SSLContext
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

  // state
  var connections: Set[ActorRef] = Set.empty

  override def preStart() {
    log.debug("attempting to bind to {}", localAddr)
    akka.io.IO(Tcp) ! Bind(self, localAddr)
  }

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
              new ProcessMessage() >>
                new ProcessFrames() >>
                new ProcessTcp() >>
                new TcpReadWriteAdapter() >>
                new SslTlsSupport(sslEngine) >>
                new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000)
            case None =>
              new ProcessMessage() >>
                new ProcessFrames() >>
                new ProcessTcp() >>
                new TcpReadWriteAdapter() >>
                new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000)
          }
          val init = SyslogPipelineHandler.init(log, stages, settings.maxMessageSize)
          val handler = context.actorOf(TcpConnectionHandler.props(init, connection, eventRouter, name, settings.idleTimeout))
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

  def props(name: String, settings: SyslogTcpSourceSettings, zookeeperPath: String, eventRouter: ActorRef) = {
    Props(classOf[SyslogTcpSource], name, settings, zookeeperPath, eventRouter)
  }

  def create(zookeeper: CuratorFramework, name: String, settings: SyslogTcpSourceSettings, eventRouter: ActorRef)(implicit factory: ActorRefFactory): SourceRef = {
    val path = "/sources/" + URLEncoder.encode(name, "UTF-8")
    val bytes = SourceSettingsFormat.write(settings).prettyPrint.getBytes(Zookeeper.UTF_8_CHARSET)
    zookeeper.create().forPath(path, bytes)
    val stat = zookeeper.checkExists().forPath(path)
    val actor = factory.actorOf(props(name, settings, path, eventRouter))
    SourceRef(actor, Source(stat, settings))
  }

  def open(zookeeper: CuratorFramework, name: String, settings: SyslogTcpSourceSettings, eventRouter: ActorRef)(implicit factory: ActorRefFactory): SourceRef = {
    val path = "/sources/" + URLEncoder.encode(name, "UTF-8")
    val stat = zookeeper.checkExists().forPath(path)
    val actor = factory.actorOf(props(name, settings, path, eventRouter))
    SourceRef(actor, Source(stat, settings))
  }
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
class SyslogPipelineHandler(init: SyslogInit, conn: ActorRef, handler: ActorRef) extends TcpPipelineHandler[SyslogContext,SyslogProcessingEvent,SyslogProcessingEvent](init, conn, handler)

object SyslogPipelineHandler {

  type SyslogInit = Init[SyslogContext, SyslogProcessingEvent, SyslogProcessingEvent]
  type SyslogPipelineStage = PipelineStage[SyslogContext, SyslogProcessingEvent, Tcp.Command, SyslogProcessingEvent, Tcp.Event]

  def init(log: LoggingAdapter, stages: SyslogPipelineStage, maxMessageSize: Option[Long]): SyslogInit = {
    new SyslogInit(stages) {
      override def makeContext(ctx: ActorContext): SyslogContext = new SyslogContext(log, ctx, maxMessageSize)
    }
  }
}

/**
 * actor responsible for managing events from a single TCP connection.
 */
class TcpConnectionHandler(init: SyslogInit, connection: ActorRef, eventRouter: ActorRef, defaultSink: String, idleTimeout: Option[FiniteDuration]) extends Actor with ActorLogging {
  import init._
  import Tcp.{ConnectionClosed, Abort, Close}
  import context.dispatcher

  var idleTimer: Option[Cancellable] = idleTimeout match {
    case Some(timeout) =>
      Some(context.system.scheduler.scheduleOnce(timeout, connection, Close))
    case None => None
  }

  def receive = {

    case Event(SyslogEvent(event)) =>
      log.debug("received {}", event)
      for (cancellable <- idleTimer) { cancellable.cancel() }
      eventRouter ! StoreEvent(defaultSink, event)
      idleTimer = idleTimeout match {
        case Some(timeout) =>
          Some(context.system.scheduler.scheduleOnce(timeout, connection, Close))
        case None => None
      }

    case Event(SyslogProcessingIncomplete) =>
      // ignore SyslogIncomplete messages

    case Event(failure: SyslogProcessingFailure) =>
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
