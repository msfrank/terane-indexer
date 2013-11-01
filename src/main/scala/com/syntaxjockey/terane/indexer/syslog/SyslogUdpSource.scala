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

package com.syntaxjockey.terane.indexer.syslog

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
class SyslogUdpSource(config: Config, eventRouter: ActorRef) extends Actor with SyslogReceiver with ActorLogging with Instrumented {
  import EventRouter._
  import context.system

  // metrics
  val messagesReceived = metrics.meter("messages-received", "messages")
  val messagesDropped = metrics.meter("messages-dropped", "messages")

  // config
  val syslogPort = config.getInt("port")
  val syslogInterface = config.getString("interface")
  val defaultSink = config.getString("use-sink")
  val allowSinkRouting = config.getBoolean("allow-sink-routing")
  val allowSinkCreate = config.getBoolean("allow-sink-creation")

  val localAddr = new InetSocketAddress(syslogInterface, syslogPort)
  log.debug("attempting to bind to {}", localAddr)
  IO(Udp) ! Bind(self, localAddr)

  val stages = new ProcessFrames() >> new ProcessUdp()
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
          case message: Message =>
            log.debug("received {}", message)
            eventRouter ! StoreEvent(defaultSink, message)
            messagesReceived.mark()
          case failure: SyslogFailure =>
            messagesDropped.mark()
            log.debug("failed to process UDP message: {}", failure.getCause.getMessage)
          case _ => // ignore other events
        }
      }
  }
}

object SyslogUdpSource {
  def props(config: Config, eventRouter: ActorRef) = Props(classOf[SyslogUdpSource], config, eventRouter)
}
