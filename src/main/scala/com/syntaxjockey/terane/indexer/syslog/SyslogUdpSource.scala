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
import akka.actor.{Props, ActorRef, Actor, ActorLogging}
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

object SyslogUdpSource {
  def props(config: Config, eventRouter: ActorRef) = Props(classOf[SyslogUdpSource], config, eventRouter)
}
