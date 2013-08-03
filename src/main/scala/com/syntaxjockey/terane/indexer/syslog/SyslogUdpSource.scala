package com.syntaxjockey.terane.indexer.syslog

import com.typesafe.config.Config
import akka.io.{PipelinePorts, PipelineFactory, Udp, IO}
import akka.actor.{ActorRef, Actor, ActorLogging}
import java.net.InetSocketAddress

import com.syntaxjockey.terane.indexer.EventRouter

class SyslogUdpSource(config: Config, eventRouter: ActorRef) extends Actor with SyslogReceiver with ActorLogging {
  import EventRouter._
  import akka.io.Udp._
  import context.system

  val syslogPort = config.getInt("port")
  val syslogInterface = config.getString("interface")
  val defaultSink = config.getString("use-sink")
  val allowSinkRouting = config.getBoolean("allow-sink-routing")
  val allowSinkCreate = config.getBoolean("allow-sink-creation")

  val localAddr = new InetSocketAddress(syslogInterface, syslogPort)
  log.debug("attempting to bind to {}", localAddr)
  IO(Udp) ! Bind(self, localAddr)

  val stages = new ProcessBody()
  val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(new SyslogContext(), stages)

  def receive = {
    case Bound(_localAddr) =>
      log.debug("bound to {}", _localAddr)
    case CommandFailed(b: Bind) =>
      log.error("failed to bind to {}", b.localAddress)
    case CommandFailed(command) =>
      log.error("{} command failed", command)
    case Received(data, remoteAddr) =>
      val (messages,_) = evt(data)
      for (message <- messages) {
        log.debug("received {}", message)
        eventRouter ! StoreEvent(defaultSink, message)
      }
  }
}

