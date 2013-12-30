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

package com.syntaxjockey.terane.indexer

import akka.actor._
import akka.util.Timeout
import spray.json.JsonParser
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import java.util.UUID
import java.net.URLDecoder

import com.syntaxjockey.terane.indexer.SourceManager.{SourceManagerData, SourceManagerState}
import com.syntaxjockey.terane.indexer.source._
import com.syntaxjockey.terane.indexer.source.SourceSettings.SourceSettingsFormat
import com.syntaxjockey.terane.indexer.zookeeper.{Zookeeper, ZNode}

/**
 * The SourceManager is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for accepting events into the cluster and passing them to the EventRouter
 * for classification.
 */
class SourceManager(supervisor: ActorRef, eventRouter: ActorRef) extends Actor with ActorLogging with FSM[SourceManagerState,SourceManagerData] {
  import SourceManager._

  // config
  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client
  implicit val timeout = Timeout(10 seconds)

  // state
  var sources: Map[String,SourceRef] = Map.empty

  context.system.eventStream.subscribe(self, classOf[SourceBroadcastOperation])

  startWith(Ready, WaitingForOperation)

  override def preStart() {
    /* ensure that /sources znode exists */
    if (zookeeper.checkExists().forPath("/sources") == null)
      zookeeper.create().forPath("/sources")
  }

  when(Ready) {
    /* load sinks from zookeeper synchronously */
    case Event(NodeBecomesLeader, _) =>
      sources = zookeeper.getChildren.forPath("/sources").map { child =>
        val path = "/sources/" + child
        val name = URLDecoder.decode(child, "UTF-8")
        val bytes = new String(zookeeper.getData.forPath(path), Zookeeper.UTF_8_CHARSET)
        val sourceSettings = SourceSettingsFormat.read(JsonParser(bytes))
        val sourceref = sourceSettings match {
          case settings: SyslogUdpSourceSettings =>
            SyslogUdpSource.open(zookeeper.usingNamespace(path), name, settings, eventRouter)
          case settings: SyslogTcpSourceSettings =>
            SyslogTcpSource.open(zookeeper.usingNamespace(path), name, settings, eventRouter)
        }
        log.debug("initialized source %s", name)
        name -> sourceref
      }.toMap
      goto(ClusterLeader)

    /* wait for the leader to give us the list of sinks */
    case Event(NodeBecomesWorker, _) =>
      goto(ClusterWorker)

  }

  when(ClusterLeader) {
    case Event(_, _) =>
      stay()
  }

  when(ClusterWorker) {
    case Event(_, _) =>
      stay()
  }

  initialize()
}

object SourceManager {
  def props(supervisor: ActorRef, eventRouter: ActorRef) = Props(classOf[SourceManager], supervisor, eventRouter)

  sealed trait SourceManagerState
  case object Ready extends SourceManagerState
  case object ClusterLeader extends SourceManagerState
  case object ClusterWorker extends SourceManagerState

  sealed trait SourceManagerData
  case object WaitingForOperation extends SourceManagerData
  case class PendingOperations(current: (SourceOperation,ActorRef), queue: Vector[(SourceOperation,ActorRef)]) extends SourceManagerData
}

case class Source(znode: ZNode, settings: SourceSettings)
case class SourceRef(actor: ActorRef, source: Source)
case class SourceMap(sources: Map[String,SourceRef])

sealed trait SourceOperationResult
sealed trait SourceCommandResult extends SourceOperationResult
sealed trait SourceQueryResult extends SourceOperationResult
case class SourceOperationFailed(cause: Throwable, op: SourceOperation) extends SourceOperationResult
case class SourceBroadcastOperation(caller: ActorRef, op: SourceOperation)

case class CreatedSource(op: CreateSource, result: SourceRef) extends SourceCommandResult
case class DeletedSource(op: DeleteSource) extends SourceCommandResult
case class EnumeratedSources(stores: Seq[SourceRef]) extends SourceQueryResult
