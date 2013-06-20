package com.syntaxjockey.terane.indexer.syslog

import akka.io.{PipelinePorts, PipelineFactory, Udp, IO}
import akka.actor.{ActorRef, Actor, ActorLogging, Props}
import java.net.InetSocketAddress
import com.syntaxjockey.terane.indexer.EventRouter

class SyslogUdpSource(eventRouter: ActorRef) extends Actor with SyslogReceiver with ActorLogging {
  import EventRouter._
  import akka.io.Udp._
  import context.system

  val localAddr = new InetSocketAddress("localhost", 10514)
  log.debug("attempting to bind to {}", localAddr)
  IO(Udp) ! Bind(self, localAddr)

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
      for (message <- messages) {
        log.debug("received {}", message)
        eventRouter ! StoreEvent("main", message2event(message))
      }
  }
}

