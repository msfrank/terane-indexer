package com.syntaxjockey.terane.indexer.syslog

import akka.io.{PipelinePorts, PipelineFactory, Udp, IO}
import akka.actor.{Actor, ActorLogging, Props}
import java.net.InetSocketAddress

class SyslogUdpSource(addr: InetSocketAddress) extends Actor with ActorLogging {
  import akka.io.Udp._
  import context.system

  log.debug("attempting to bind to {}", addr)
  IO(Udp) ! Bind(self, addr)

  val stages = new ProcessBody()
  val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(new SyslogContext(), stages)

  def receive = {
    case Bound(localAddr) =>
      log.debug("bound to {}", localAddr)
    case CommandFailed(b: Bind) =>
      log.error("failed to bind to {}", b.endpoint)
    case CommandFailed(command) =>
      log.error("{} command failed", command)
    case Received(data, remoteAddr) =>
      val (messages,_) = evt(data)
      for (message <- messages)
        log.debug("received {}", message)
  }
}

