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

package com.syntaxjockey.terane.indexer.http

import akka.io.IO
import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import spray.routing.{PathMatcher, ExceptionHandler, HttpService}
import spray.can.Http
import spray.http._
import spray.http.HttpHeaders.Location
import spray.util.LoggingContext
import akka.util.Timeout._
import java.util.UUID

import com.syntaxjockey.terane.indexer.sink.Query._
import com.syntaxjockey.terane.indexer.metadata.StoreManager._
import com.syntaxjockey.terane.indexer.sink.CassandraSink._
import com.syntaxjockey.terane.indexer._
import com.syntaxjockey.terane.indexer.metadata.StoreManager.StoreStatistics
import com.syntaxjockey.terane.indexer.sink.Query.GetEvents
import com.syntaxjockey.terane.indexer.metadata.StoreManager.DescribeStore
import com.syntaxjockey.terane.indexer.metadata.StoreManager.CreateStore
import com.syntaxjockey.terane.indexer.metadata.StoreManager.CreatedStore
import com.syntaxjockey.terane.indexer.sink.Query.EventSet
import spray.http.HttpResponse
import com.syntaxjockey.terane.indexer.metadata.StoreManager.DeleteStore
import com.syntaxjockey.terane.indexer.sink.Query.QueryStatistics
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreatedQuery

// see http://stackoverflow.com/questions/15584328/scala-future-mapto-fails-to-compile-because-of-missing-classtag
import reflect.ClassTag

/**
 * HttpServer is responsible for listening on the HTTP port, accepting connections,
 * and handing them over to the ApiService for processing.
 */
class HttpServer(val settings: HttpSettings, val eventRouter: ActorRef) extends Actor with ApiService with ActorLogging {

  val timeout: Timeout = settings.requestTimeout

  implicit val system = context.system
  implicit val dispatcher = context.dispatcher
  val actorRefFactory = context

  override def preStart() {
    IO(Http) ! Http.Bind(self, settings.interface, port = settings.port, backlog = settings.backlog)
    log.debug("binding to %s:%d with backlog %d".format(settings.interface, settings.port, settings.backlog))
  }

  def receive = runRoute(routes) orElse {
    case bound: Http.Bound => log.debug("bound HTTP listener to {}", bound.localAddress)
  }
}

object HttpServer {
  def props(settings: HttpSettings, eventRouter: ActorRef) = Props(classOf[HttpServer], settings, eventRouter)
}

/**
 * ApiService contains the REST API logic.
 */
trait ApiService extends HttpService {
  import scala.language.postfixOps
  import JsonProtocol._
  import spray.httpx.SprayJsonSupport._
  import spray.json._

  val settings: HttpSettings

  implicit def log: LoggingAdapter
  implicit def eventRouter: ActorRef
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout

  def throwableToJson(t: Throwable): JsValue = JsObject(Map("description" -> JsString(t.getMessage)))

  /**
   * catch thrown exceptions and convert them to HTTP responses.  we bake in support
   * for catching APIFailure objects wrapped in an APIException, otherwise any other
   * Throwable results in a generic 500 Internal Server Error.
   */
  implicit def exceptionHandler(implicit log: LoggingContext) = ExceptionHandler {
    case ex: ApiException => ctx =>
      log.error(ex, "caught API exception in spray routing: %s".format(ex.getMessage))
      ex.failure match {
        case failure: RetryLater =>
          ctx.complete(HttpResponse(StatusCodes.ServiceUnavailable, JsonBody(throwableToJson(ex))))
        case failure: BadRequest =>
          ctx.complete(HttpResponse(StatusCodes.BadRequest, JsonBody(throwableToJson(ex))))
        case failure: ResourceNotFound =>
          ctx.complete(HttpResponse(StatusCodes.NotFound, JsonBody(throwableToJson(ex))))
        case _ =>
          ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(ex))))
      }
    case ex: Throwable => ctx =>
      log.error(ex, "caught exception in spray routing: %s".format(ex.getMessage))
      ctx.complete(HttpResponse(StatusCodes.InternalServerError, JsonBody(throwableToJson(new Exception("internal server error")))))
  }

  /**
   * Spray routes for manipulating queries (create, describe, get events, delete)
   */
  val queriesRoutes = {
    path("1" / "queries") {
      post {
        hostName { hostname =>
          entity(as[CreateQuery]) { case createQuery: CreateQuery =>
            complete {
              log.debug("creating query")
              eventRouter.ask(createQuery).map {
                case createdQuery: CreatedQuery =>
                  HttpResponse(StatusCodes.Created,
                    JsonBody(createdQuery.toJson),
                    List(Location("http://%s:%d/1/queries/%s".format(hostname, settings.port, createdQuery.id))))
                case failure: ApiFailure =>
                  log.info("error creating query")
                  throw new ApiException(failure)
              }.mapTo[HttpResponse]
            }
          }
        }
      }
    } ~
    pathPrefix("1" / "queries" / JavaUUID) { case id: UUID =>
      pathEndOrSingleSlash {
        get {
          complete {
            actorRefFactory.actorSelection("/user/query-" + id).ask(DescribeQuery).map {
                case queryStatistics: QueryStatistics =>
                  queryStatistics
                case failure: ApiFailure =>
                  throw new ApiException(failure)
              }.mapTo[QueryStatistics]
          }
        } ~
        delete {
          complete {
            actorRefFactory.actorSelection("/user/query-" + id).ask(DeleteQuery).map {
              case failure: ApiFailure =>
                throw new ApiException(failure)
              case _ => StatusCodes.Accepted
            }.mapTo[HttpResponse]
          }
        }
      } ~
      path("events") {
        pathEndOrSingleSlash {
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
                }.mapTo[EventSet]
              }
            }}
          }
        }
      }
    }
  }

  val storeId = PathMatcher("""[a-z0-9]{32}""".r)

  /**
   * Spray routes for manipulating stores (create, describe, delete)
   */
  val storesRoutes = {
    path("1" / "stores") {
      pathEndOrSingleSlash {
        get { parameters("name" ?) {
          case Some(name) =>
            hostName { hostname => complete {
              eventRouter.ask(FindStore(name)).map {
                case storeStatistics: StoreStatistics =>
                  HttpResponse(StatusCodes.SeeOther,
                    JsonBody(storeStatistics.toJson),
                    List(Location("http://%s:%d/1/stores/%s".format(hostname, settings.port, storeStatistics.id))))
                case failure: ApiFailure =>
                  throw new ApiException(failure)
              }.mapTo[HttpResponse]
            }}
          case _ =>
            complete {
              eventRouter.ask(EnumerateStores).map {
                case enumeratedStores: EnumeratedStores =>
                  enumeratedStores
                case failure: ApiFailure =>
                  throw new ApiException(failure)
              }.mapTo[EnumeratedStores]
            }
        }} ~
        post {
          hostName { hostname =>
            entity(as[CreateStore]) { case createStore: CreateStore =>
              complete {
                eventRouter.ask(createStore).map {
                  case CreatedStore(op, store) =>
                    HttpResponse(StatusCodes.Created,
                      JsonBody(store.toJson),
                      List(Location("http://%s:%d/1/stores/%s".format(hostname, settings.port, store.id))))
                  case failure: ApiFailure =>
                    throw new ApiException(failure)
                }.mapTo[HttpResponse]
              }
            }
          }
        }
      }
    } ~
    pathPrefix("1" / "stores" / storeId) { case id: String =>
      pathEndOrSingleSlash {
        get {
          complete {
            eventRouter.ask(DescribeStore(id)).map {
              case storeStatistics: StoreStatistics =>
                storeStatistics
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }.mapTo[StoreStatistics]
          }
        } ~
        delete {
          complete {
            eventRouter.ask(DeleteStore(id)).map {
              case failure: ApiFailure =>
                throw new ApiException(failure)
              case _ => StatusCodes.OK
            }.mapTo[HttpResponse]
          }
        }
      }
    }
  }

  val version1 = queriesRoutes ~ storesRoutes

  val routes =  version1
}

