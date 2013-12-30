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

import akka.io.{PipelinePorts, PipelineFactory, Udp, IO}
import akka.io.Udp.{Bind, CommandFailed, Bound, Received}
import akka.actor._
import org.apache.curator.framework.CuratorFramework
import java.net.{URLEncoder, InetSocketAddress}

import com.syntaxjockey.terane.indexer.{Source, SourceRef, Instrumented, EventRouter}
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper

/**
 * Actor implementing the syslog protocol over UDP in accordance with RFC5424:
 * http://tools.ietf.org/html/rfc5424
 */
class SyslogUdpSource(name: String, settings: SyslogUdpSourceSettings, zookeeper: CuratorFramework, eventRouter: ActorRef) extends Actor
with ActorLogging with Instrumented {
  import EventRouter._
  import context.system

  // metrics
  val messagesReceived = metrics.meter("messages-received", "messages")
  val messagesDropped = metrics.meter("messages-dropped", "messages")

  // config
  val localAddr = new InetSocketAddress(settings.interface, settings.port)
  val stages = new ProcessMessage >> new ProcessFrames() >> new ProcessUdp()
  val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(new SyslogContext(log, context), stages)

  override def preStart() {
    log.debug("attempting to bind to {}", localAddr)
    IO(Udp) ! Bind(self, localAddr)
  }

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
            eventRouter ! StoreEvent(name, _event)
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
  import SyslogUdpSourceSettings.SyslogUdpSourceSettingsFormat

  def props(name: String, zookeeper: CuratorFramework, eventRouter: ActorRef, settings: SyslogUdpSourceSettings) = {
    Props(classOf[SyslogUdpSource], name, settings, zookeeper, eventRouter)
  }

  def create(zookeeper: CuratorFramework, name: String, settings: SyslogUdpSourceSettings, eventRouter: ActorRef)(implicit factory: ActorRefFactory): SourceRef = {
    val path = "/" + URLEncoder.encode(name, "UTF-8")
    val bytes = SyslogUdpSourceSettingsFormat.write(settings).prettyPrint.getBytes(Zookeeper.UTF_8_CHARSET)
    zookeeper.create().forPath(path, bytes)
    val stat = zookeeper.checkExists().forPath(path)
    val actor = factory.actorOf(props(name, zookeeper.usingNamespace(path), eventRouter, settings))
    SourceRef(actor, Source(stat, settings))
  }

  def open(zookeeper: CuratorFramework, name: String, settings: SyslogUdpSourceSettings, eventRouter: ActorRef)(implicit factory: ActorRefFactory): SourceRef = {
    val path = "/" + URLEncoder.encode(name, "UTF-8")
    val stat = zookeeper.checkExists().forPath(path)
    val actor = factory.actorOf(props(name, zookeeper.usingNamespace(path), eventRouter, settings))
    SourceRef(actor, Source(stat, settings))
  }
}
