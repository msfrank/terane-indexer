package com.syntaxjockey.terane.indexer

import akka.actor.{Props, Actor, ActorLogging, ActorRef}
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import spray.json._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.sink.{CassandraSink, CassandraSinkSettings, SinkSettings}
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper

/**
 * The SinkManager manages sinks in a cluster, acting as a router for all sink operations.
 */
class SinkManager(supervisor: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher

  val settings = IndexerConfig(context.system).settings
  val zookeeper = Zookeeper(context.system).client

  implicit val timeout = Timeout(10 seconds)

  // state
  var sinks = SinkMap(Map.empty, Map.empty)
  var pending: Map[SinkOperation,ActorRef] = Map.empty
  var isLeader = false

  context.system.eventStream.subscribe(self, classOf[SinkBroadcastOperation])

  override def preStart() {
    /* ensure that /sinks znode exists */
    zookeeper.create().forPath("/sinks")
  }

  def receive = {

    case NodeBecomesLeader =>
      isLeader = true

    case NodeBecomesWorker =>
      isLeader = false

    /* list all known sinks */
    case EnumerateSinks =>
      sender ! EnumeratedSinks(sinks.sinksById.values.toSeq)

    /* create a new sink, if it doesn't exist already */
    case op: CreateSink if isLeader =>
      val id = UUID.randomUUID()
      if (pending.contains(op)) {
        sender ! SinkOperationFailed(new Exception("%s is already in progress".format(op)), op)
      } else if (sinks.sinksByName.contains(op.settings.name)) {
        sender ! SinkOperationFailed(new Exception("sink %s already exists".format(op.settings.name)), op)
      } else {
        pending = pending + (op -> sender)
        createSink(op, id) pipeTo self
      }

    case result @ CreatedSink(createSink, sink) =>
      pending.get(createSink) match {
        case Some(caller) =>
          caller ! result
          pending = pending - createSink
        case None =>  // FIXME: do we do anything here?
      }
      val SinkMap(sinksById, sinksByName) = sinks
      sinks = SinkMap(sinksById + (sink.id -> sink), sinksByName + (sink.settings.name -> sink))

    /* delete a sink, if it exists */
    case deleteSink: DeleteSink =>
      sender ! SinkOperationFailed(new NotImplementedError("DeleteSink not implemented"), deleteSink)

    /* describe a sink, if it exists */
    case describeSink: DescribeSink =>
      sender ! SinkOperationFailed(new NotImplementedError("DescribeSink not implemented"), describeSink)

    /* describe a sink if it exists */
    case findSink: FindSink =>
      sender ! SinkOperationFailed(new NotImplementedError("FindSink not implemented"), findSink)

    /* an asynchronous operation failed */
    case failure @ SinkOperationFailed(cause, op) =>
      log.error("operation {} failed: {}", op, cause.getMessage)
      pending.get(op) match {
        case Some(caller) =>
          caller ! failure
          pending = pending - op
        case None =>  // FIXME: do we do anything here?
      }

    /* forward the operation to self on behalf of caller */
    case SinkBroadcastOperation(caller, op) =>
      self.tell(op, caller)
  }

  /**
   *
   */
  def createSink(op: CreateSink, id: UUID): Future[SinkOperationResult] = Future {
    val path = "/sinks/" + id.toString
    // FIXME: lock znode
    val actor = op.settings match {
      case cassandraSinkSettings: CassandraSinkSettings =>
        zookeeper.create().forPath(path, cassandraSinkSettings.toJson.prettyPrint.getBytes)
        context.actorOf(CassandraSink.props(id, cassandraSinkSettings, zookeeper.usingNamespace(path)))
    }
    val sink = Sink(actor, id, op.settings)
    // blocking on a future is ugly, but seems like the most straightforward way in this case
    Await.result(actor ? SinkManager.Create, 10 seconds) match {
      case SinkManager.Done =>  // do nothing
      case SinkManager.Failure(ex) => throw ex
    }
    log.debug("created sink %s (%s): %s", op.settings.name, id, op.settings)
    // FIXME: perform cleanup if any part of creation fails
    CreatedSink(op, sink)
  }.recover {
    case ex: Throwable =>
      SinkOperationFailed(ex, op)
  }
}

object SinkManager {
  def props(supervisor: ActorRef) = Props(classOf[SinkManager], supervisor)
  case object Open
  case object Create
  case object Done
  case class Failure(ex: Throwable)
}

case class Sink(actor: ActorRef, id: UUID, settings: SinkSettings)
case class SinkMap(sinksById: Map[UUID,Sink], sinksByName: Map[String,Sink])

sealed trait SinkOperationResult
sealed trait SinkCommandResult extends SinkOperationResult
sealed trait SinkQueryResult extends SinkOperationResult
case class SinkOperationFailed(cause: Throwable, op: SinkOperation) extends SinkOperationResult
case class SinkBroadcastOperation(caller: ActorRef, op: SinkOperation)

case class CreatedSink(op: CreateSink, result: Sink) extends SinkCommandResult
case class DeletedSink(op: DeleteSink, result: Sink) extends SinkCommandResult
case class EnumeratedSinks(sinks: Seq[Sink]) extends SinkQueryResult
