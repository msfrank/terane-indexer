package com.syntaxjockey.terane.indexer

import akka.actor.{Props, Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.sink.CassandraSink
import com.syntaxjockey.terane.indexer.bier.Event
import java.util.UUID
import com.syntaxjockey.terane.indexer.EventRouter.{QueryEvents, GetEvent}

class EventRouter extends Actor with ActorLogging {
  val csSink = context.actorOf(Props(new CassandraSink("store")), "cassandra-sink")

  def receive = {
    case event: Event =>
      log.debug("forwarding event from {} to {}", sender.path.name, csSink.path.name)
      csSink forward event
    case get: GetEvent =>
      csSink forward get
    case query: QueryEvents =>
      csSink forward query
  }
}

object EventRouter {
  case class GetEvent(id: UUID)
  case class QueryEvents(query: String)
}