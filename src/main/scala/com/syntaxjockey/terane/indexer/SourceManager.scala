package com.syntaxjockey.terane.indexer

import akka.actor.{Props, Actor, ActorLogging, ActorRef}
import org.joda.time.DateTime
import java.util.UUID

import com.syntaxjockey.terane.indexer.source._

/**
 * The SourceManager is a second-level actor (underneath ClusterSupervisor) which
 * is responsible for accepting events into the cluster and passing them to the EventRouter
 * for classification.
 */
class SourceManager(supervisor: ActorRef, eventRouter: ActorRef) extends Actor with ActorLogging {

  val settings = IndexerConfig(context.system).settings

  var sourceMap = SourceMap(Map.empty, Map.empty)

  context.system.eventStream.subscribe(self, classOf[SourceBroadcastOperation])

  override def preStart() {
    /* start any locally-defined sources */
    val sourcesById: Map[UUID,Source] = settings.sources.map { case (name: String, sourceSettings: SourceSettings) =>
      val id = UUID.nameUUIDFromBytes("source:%s:%s".format(settings.nodeId, name).getBytes)
      val actor = sourceSettings match {
        case syslogTcpSourceSettings: SyslogTcpSourceSettings =>
          context.actorOf(SyslogTcpSource.props(syslogTcpSourceSettings, eventRouter), "source-" + name)
        case syslogUdpSourceSettings: SyslogUdpSourceSettings =>
          context.actorOf(SyslogUdpSource.props(syslogUdpSourceSettings, eventRouter), "source-" + name)
      }
      val source = Source(actor, id, name, sourceSettings, true)
      log.debug("started local source %s (%s): %s", name, id, sourceSettings)
      source.id -> source
    }.toMap
    sourceMap = SourceMap(sourcesById, sourcesById.values.map(s => s.name -> s).toMap)
  }

  def receive = {

    /* forward the operation to self on behalf of caller */
    case SourceBroadcastOperation(caller, op) =>
      self.tell(op, caller)
  }
}

object SourceManager {
  def props(supervisor: ActorRef, eventRouter: ActorRef) = Props(classOf[SourceManager], supervisor, eventRouter)
}

case class Source(actor: ActorRef, id: UUID, name: String, settings: SourceSettings, isLocal: Boolean)
case class SourceMap(sourcesById: Map[UUID,Source], sourcesByName: Map[String,Source])

sealed trait SourceOperation
sealed trait SourceOperationResult
case class SourceBroadcastOperation(caller: ActorRef, op: SourceOperation)
sealed trait SourceCommand extends SourceOperation
sealed trait SourceCommandResult extends SourceOperationResult
sealed trait SourceQuery extends SourceOperation
sealed trait SourceQueryResult extends SourceOperationResult

case class CreateSource(name: String) extends SourceCommand
case class CreatedStore(op: CreateSource, result: Source) extends SourceCommandResult

case class DeleteSource(id: String) extends SourceCommand
case class DeletedSource(op: DeleteSource, result: Source) extends SourceCommandResult

case class SourceOperationFailed(cause: Throwable, op: SourceOperation) extends SourceOperationResult

case object EnumerateSources extends SourceQuery
case class EnumeratedSources(stores: Seq[SourceStatistics]) extends SourceQueryResult
case class FindSource(name: String) extends SourceQuery
case class DescribeSource(id: String) extends SourceQuery
case class SourceStatistics(id: String, name: String, created: DateTime) extends SourceQueryResult