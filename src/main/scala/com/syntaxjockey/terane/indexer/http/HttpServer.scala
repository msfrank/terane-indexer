package com.syntaxjockey.terane.indexer.http

import akka.io.IO
import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import spray.routing.{ExceptionHandler, HttpService}
import spray.can.Http
import spray.http._
import spray.http.HttpHeaders.Location
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.sink.CassandraSink._
import com.syntaxjockey.terane.indexer.sink.Query._
import spray.json._
import com.syntaxjockey.terane.indexer.sink.Query.GetEvents
import com.syntaxjockey.terane.indexer.sink.Query.EventSet
import spray.http.HttpResponse
import com.syntaxjockey.terane.indexer.sink.Query.QueryStatistics
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreatedQuery
import spray.util.LoggingContext

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

  def throwableToJson(t: Throwable): JsValue = JsObject(Map("description" -> JsString(t.getMessage)))

  implicit def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case ex: ApiException => ctx =>
      val entity = throwableToJson(ex).toString()
      ex.failure match {
        case failure: RetryLater =>
          ctx.complete(HttpResponse(StatusCodes.Accepted, HttpEntity(MediaTypes.`application/json`, entity)))
        case failure: BadRequest =>
          ctx.complete(HttpResponse(StatusCodes.BadRequest, HttpEntity(MediaTypes.`application/json`, entity)))
        case _ =>
          ctx.complete(HttpResponse(StatusCodes.InternalServerError, HttpEntity(MediaTypes.`application/json`, entity)))
      }
    case ex: Throwable => ctx =>
      log.error(ex, "caught exception in spray routing")
      ctx.complete(HttpResponse(StatusCodes.InternalServerError, HttpEntity(MediaTypes.`application/json`, "internal server error")))
  }

  val routes = {
    path("1" / "queries") {
      post {
        entity(as[CreateQuery]) { createQuery =>
          complete {
            eventRouter.ask(createQuery).map {
              case createdQuery: CreatedQuery =>
                HttpResponse(StatusCodes.Created,
                  HttpEntity(MediaTypes.`application/json`, createdQuery.toJson.toString()),
                  List(Location("http://localhost:8080/1/queries/" + createdQuery.id)))
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }
          }
        }
      }
    } ~
    pathPrefix("1" / "queries" / JavaUUID) { case id: UUID =>
      path("") {
        get {
          complete {
            actorRefFactory.actorSelection("/user/query-" + id).ask(DescribeQuery).map {
                case queryStatistics: QueryStatistics =>
                  queryStatistics
                case failure: ApiFailure =>
                  throw new ApiException(failure)
              }
          }
        } ~
        delete {
          complete {
            actorRefFactory.actorSelection("/user/query-" + id).ask(DeleteQuery).map {
              case failure: ApiFailure =>
                throw new ApiException(failure)
              case _ => StatusCodes.Accepted
            }
          }
        }
      } ~
      path("events") {
        get {
          parameter('offset.as[Int] ?) { offset =>
          parameter('limit.as[Int] ?) { limit =>
            val getEvents = GetEvents(offset, limit)
            complete {
              actorRefFactory.actorSelection("/user/query-" + id).ask(getEvents).map {
                  case eventSet: EventSet =>
                    eventSet
                  case failure: ApiFailure =>
                    throw new ApiException(failure)
                }
            }
          }}
        }
      }
    }
  }
}

abstract class ApiFailure(val description: String)
abstract class RetryLater(_description: String) extends ApiFailure(_description)
abstract class BadRequest(_description: String) extends ApiFailure(_description)

class ApiException(val failure: ApiFailure) extends Exception(failure.description)
