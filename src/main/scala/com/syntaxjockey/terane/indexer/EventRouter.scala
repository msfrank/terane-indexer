package com.syntaxjockey.terane.indexer

import java.net.InetSocketAddress
import akka.actor.{ActorRef, Props, Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.syslog.SyslogUdpSource
import com.syntaxjockey.terane.indexer.bier.Event

class EventRouter(sink: ActorRef) extends Actor with ActorLogging {
  val localAddr = new InetSocketAddress("localhost", 10514)
  val syslogSource = context.actorOf(Props(new SyslogUdpSource(localAddr)), "syslog-udp-source")

  def receive = {
    case event: Event =>
      log.debug("forwarding event from {} to {}", sender.path.name, sink.path.name)
      sink forward event
  }

}
