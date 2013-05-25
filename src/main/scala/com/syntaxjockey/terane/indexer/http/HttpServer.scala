package com.syntaxjockey.terane.indexer.http

import spray.routing.HttpService
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import scala.concurrent.duration._
import akka.util.Timeout
import com.syntaxjockey.terane.indexer.EventRouter
import com.syntaxjockey.terane.indexer.bier.Event
import spray.httpx.marshalling.Marshaller
import spray.http.{HttpEntity, HttpBody, EmptyEntity, ContentType}

// see http://stackoverflow.com/questions/15584328/scala-future-mapto-fails-to-compile-because-of-missing-classtag
import reflect.ClassTag

/**
 *
 */
class HttpServer(val eventRouter: ActorRef) extends Actor with ApiService with ActorLogging {

  val config = context.system.settings.config.getConfig("terane.http")
  val httpPort = config.getInt("port")
  val httpInterface = config.getString("interface")
  val httpBacklog = config.getInt("backlog")

  implicit val system = context.system
  implicit val dispatcher = context.dispatcher
  val actorRefFactory = context

  override def preStart() {
    log.debug("binding to %s:%d with backlog %d".format(httpInterface, httpPort, httpBacklog))
    IO(Http) ! Http.Bind(self, httpInterface, port = httpPort, backlog = httpBacklog)
  }

  def receive = runRoute(routes)
}

trait EventMarshaller {
  implicit val EventMarshaller = Marshaller.of[Option[Event]](ContentType.`text/plain`) {
    (value, contentType, ctx) =>
      val entity = if (value.isDefined)
        HttpEntity(contentType, value.get.toString())
      else EmptyEntity
      ctx.marshalTo(entity)
  }
}

trait ApiService extends HttpService with EventMarshaller {
  import EventRouter._

  implicit def eventRouter: ActorRef
  implicit def executionContext = actorRefFactory.dispatcher

  val routes = {
    path("events" / JavaUUID) { id =>
      get {
        complete {
          implicit val timeout = Timeout(1 second)
          eventRouter.ask(GetEvent(id)).mapTo[Option[Event]]
        }
      }
    }
  }
}

