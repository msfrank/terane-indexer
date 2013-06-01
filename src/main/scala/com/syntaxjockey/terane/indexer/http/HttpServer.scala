package com.syntaxjockey.terane.indexer.http

import spray.routing.HttpService
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import scala.concurrent.duration._
import akka.util.Timeout
import com.syntaxjockey.terane.indexer.EventRouter
import akka.event.LoggingAdapter
import spray.http._
import com.syntaxjockey.terane.indexer.bier.Event
import scala.util.{Failure, Success}
import spray.http.HttpHeaders.Location
import spray.http.HttpResponse
import spray.http.HttpHeaders.Location

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

trait ApiService extends HttpService {
  import EventRouter._
  import JsonProtocol._
  import spray.httpx.SprayJsonSupport._
  import spray.json._

  implicit def log: LoggingAdapter
  implicit def eventRouter: ActorRef
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout = 3.seconds

  val routes = {
    path("1" / "queries") {
      //get {
      //  complete { eventRouter.ask(ListQueries).mapTo[ListQueriesResponse] }
      //} ~
      post {
        entity(as[CreateQuery]) { createQuery =>
          complete {
            eventRouter.ask(createQuery).map({
              case result: CreateQueryResponse =>
                HttpResponse(StatusCodes.Accepted,
                  HttpEntity(MediaTypes.`application/json`, result.toJson.toString()),
                  List(Location("http://localhost:8080/1/queries/" + result.id)))
            })
          }
        }
      }
    } ~
    path("1" / "queries" / JavaUUID) { queryId =>
      get {
        complete { eventRouter.ask(DescribeQuery(queryId)).mapTo[DescribeQueryResponse] }
      } ~
      post {
        complete { eventRouter.ask(GetEvents(queryId)).mapTo[List[Event]] }
      } ~
      delete {
        complete { eventRouter.ask(DeleteQuery(queryId)).map(result => StatusCodes.Accepted) }
      }
    }
  }
}
