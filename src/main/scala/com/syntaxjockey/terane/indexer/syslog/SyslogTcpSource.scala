package com.syntaxjockey.terane.indexer.syslog

import akka.actor.{Actor, ActorRef, ActorLogging, ActorContext, Props}
import akka.io.{IO, Tcp, TcpPipelineHandler, PipelineStage}
import akka.io.TcpPipelineHandler.Init
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import java.net.InetSocketAddress

import com.syntaxjockey.terane.indexer.syslog.SyslogPipelineHandler.SyslogInit
import com.syntaxjockey.terane.indexer.EventRouter.StoreEvent

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

  val syslogPort = config.getInt("port")
  val syslogInterface = config.getString("interface")
  val enableTls = config.getBoolean("enable-tls")
  val defaultSink = config.getString("use-sink")
  val allowSinkRouting = config.getBoolean("allow-sink-routing")
  val allowSinkCreate = config.getBoolean("allow-sink-creation")

  val localAddr = new InetSocketAddress(syslogInterface, syslogPort)
  log.debug("attempting to bind to {}", localAddr)
  IO(Tcp) ! Bind(self, localAddr)


  def receive = {

    case Bound(_localAddr) =>
      log.debug("listening on {}", _localAddr)

    case CommandFailed(b: Bind) =>
      log.error("failed to bind to {}", b.localAddress)

    case CommandFailed(command) =>
      log.error("{} command failed", command)

    case Connected(remote, local) =>
      val connection = sender
      val upperStages = new ProcessFrames() >> new ProcessTcp() >> new TcpReadWriteAdapter()
      val stages = upperStages >> new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000)
      val init = SyslogPipelineHandler.init(log, stages)
      val handler = context.actorOf(TcpConnectionHandler.props(init, eventRouter, defaultSink))
      val pipeline = context.actorOf(TcpPipelineHandler.props(init, connection, handler))
      connection ! Tcp.Register(pipeline)
      log.debug("registered connection from {}", remote)
  }
}

object SyslogTcpSource {
  def props(config: Config, eventRouter: ActorRef) = Props(classOf[SyslogUdpSource], config, eventRouter)
}

class SyslogPipelineHandler(init: SyslogInit, conn: ActorRef, handler: ActorRef) extends TcpPipelineHandler[SyslogContext,SyslogMessages,SyslogMessages](init, conn, handler)

object SyslogPipelineHandler {

  type SyslogInit = Init[SyslogContext, SyslogMessages, SyslogMessages]
  type SyslogPipelineStage = PipelineStage[SyslogContext, SyslogMessages, Tcp.Command, SyslogMessages, Tcp.Event]

  def init(log: LoggingAdapter, stages: SyslogPipelineStage): SyslogInit = {
    new SyslogInit(stages) {
      override def makeContext(ctx: ActorContext): SyslogContext = new SyslogContext(log, ctx)
    }
  }
}

/**
 *
 */
class TcpConnectionHandler(init: SyslogInit, eventRouter: ActorRef, defaultSink: String) extends Actor with SyslogReceiver with ActorLogging {
  def receive = {
    case init.Event(messages) =>
      for (message <- messages.messages) {
        log.debug("received {}", message)
        eventRouter ! StoreEvent(defaultSink, message)
      }
    case _: Tcp.ConnectionClosed =>
      context.stop(self)
  }
}

object TcpConnectionHandler {
  def props(init: SyslogInit, eventRouter: ActorRef, defaultSink: String) = Props(classOf[TcpConnectionHandler], init, eventRouter, defaultSink)
}
