package com.syntaxjockey.terane.indexer.http

import akka.io.IO
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import spray.routing.HttpService
import spray.can.Http
import spray.http._
import spray.http.HttpResponse
import spray.http.HttpHeaders.Location
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.sink.CassandraSink.{CreatedQuery, CreateQuery}
import com.syntaxjockey.terane.indexer.sink.Query.{DeleteQuery, GetEvents, QueryStatistics, DescribeQuery}
import com.syntaxjockey.terane.indexer.sink.Query.EventsBatch

// see http://stackoverflow.com/questions/15584328/scala-future-mapto-fails-to-compile-because-of-missing-classtag
import reflect.ClassTag

/**
 *
 */
class HttpServer(config: Config, val eventRouter: ActorRef) extends Actor with ApiService with ActorLogging {

  val httpPort = config.getInt("port")
  val httpInterface = config.getString("interface")
  val httpBacklog = config.getInt("backlog")
  val timeout: Timeout = Timeout(config.getMilliseconds("request-timeout"), TimeUnit.MILLISECONDS)

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
  import JsonProtocol._
  import spray.httpx.SprayJsonSupport._
  import spray.json._

  implicit def log: LoggingAdapter
  implicit def eventRouter: ActorRef
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout

  val routes = {
    path("1" / "queries") {
      //get {
      //  complete { eventRouter.ask(ListQueries).mapTo[ListQueriesResponse] }
      //} ~
      post {
        entity(as[CreateQuery]) { createQuery =>
          complete {
            eventRouter.ask(createQuery).map({
              case result: CreatedQuery =>
                HttpResponse(StatusCodes.Accepted,
                  HttpEntity(MediaTypes.`application/json`, result.toJson.toString()),
                  List(Location("http://localhost:8080/1/queries/" + result.id)))
            })
          }
        }
      }
    } ~
    path("1" / "queries" / JavaUUID) { id =>
      get {
        complete { actorRefFactory.actorSelection("/user/query-" + id).ask(DescribeQuery).mapTo[QueryStatistics] }
      } ~
      post {
        entity(as[GetEvents]) { getEvents =>
          complete { actorRefFactory.actorSelection("/user/query-" + id).ask(getEvents).mapTo[EventsBatch] }
        }
      } ~
      delete {
        complete { actorRefFactory.actorSelection("/user/query-" + id).ask(DeleteQuery).map(result => StatusCodes.Accepted) }
      }
    }
  }
}
