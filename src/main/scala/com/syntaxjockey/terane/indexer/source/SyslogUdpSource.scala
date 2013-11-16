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
import akka.io.{PipelinePorts, PipelineFactory, Udp, IO}
import akka.io.Udp.{Bind, CommandFailed, Bound, Received}
import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import java.net.InetSocketAddress

import com.syntaxjockey.terane.indexer.{Instrumented, EventRouter}

/**
 * Actor implementing the syslog protocol over UDP in accordance with RFC5424:
 * http://tools.ietf.org/html/rfc5424
 */
class SyslogUdpSource(settings: SyslogUdpSourceSettings, eventRouter: ActorRef) extends Actor with ActorLogging with Instrumented {
  import EventRouter._
  import context.system

  // metrics
  val messagesReceived = metrics.meter("messages-received", "messages")
  val messagesDropped = metrics.meter("messages-dropped", "messages")

  val localAddr = new InetSocketAddress(settings.interface, settings.port)
  log.debug("attempting to bind to {}", localAddr)
  IO(Udp) ! Bind(self, localAddr)

  val stages = new ProcessMessage >> new ProcessFrames() >> new ProcessUdp()
  val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(new SyslogContext(log, context), stages)

  def receive = {
    case Bound(_localAddr) =>
      log.debug("bound to {}", _localAddr)
    case CommandFailed(b: Bind) =>
      log.error("failed to bind to {}", b.localAddress)
    case CommandFailed(command) =>
      log.error("{} command failed", command)
    case Received(data, remoteAddr) =>
      val (events,_) = evt(data)
      for (event <- events) {
        event match {
          case SyslogEvent(_event) =>
            log.debug("received {}", _event)
            eventRouter ! StoreEvent(settings.useSink, _event)
            messagesReceived.mark()
          case failure: SyslogProcessingFailure =>
            messagesDropped.mark()
            log.debug("failed to process UDP message: {}", failure.getCause.getMessage)
          case _ => // ignore other events
        }
      }
  }
}

object SyslogUdpSource {
  def props(settings: SyslogUdpSourceSettings, eventRouter: ActorRef) = Props(classOf[SyslogUdpSource], settings, eventRouter)
}