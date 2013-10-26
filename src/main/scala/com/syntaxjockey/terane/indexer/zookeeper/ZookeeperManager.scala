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

package com.syntaxjockey.terane.indexer.zookeeper

import akka.actor.{ActorRef, Actor, ActorLogging, Props}
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.{ConnectionStateListener, ConnectionState}
import com.netflix.curator.framework.api.UnhandledErrorListener

class ZookeeperListener(manager: ActorRef) extends ConnectionStateListener with UnhandledErrorListener {
  import ZookeeperManager._

  def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    manager ! StateChanged(newState)
  }

  def unhandledError(message: String, reason: Throwable) {
    manager ! UnhandledError(message, reason)
  }
}

class ZookeeperManager(client: CuratorFramework) extends Actor with ActorLogging {
  import ZookeeperManager._

  log.debug("started zookeeper manager")

  def receive = {

    case StateChanged(newState) =>
      log.debug("zookeeper state changed to {}", newState.name())

    case UnhandledError(message, reason) =>
      log.error("caught unhandled error from zookeeper: {}", message)

  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.warning("restarted zookeeper manager")
  }

  override def postStop() {
    client.close()
    log.debug("stopped zookeeper manager")
  }
}

object ZookeeperManager {
  def props(client: CuratorFramework) = Props(classOf[ZookeeperManager], client)

  case class StateChanged(newState: ConnectionState)
  case class UnhandledError(message: String, reason: Throwable)
}
