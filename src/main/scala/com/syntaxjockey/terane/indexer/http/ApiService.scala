/*
 * *
 *  * Copyright (c) 2010-${YEAR} Michael Frank <msfrank@syntaxjockey.com>
 *  *
 *  * This file is part of Terane.
 *  *
 *  * Terane is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Terane is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.syntaxjockey.terane.indexer.http

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import akka.event.LoggingAdapter
import spray.routing.{HttpService, ExceptionHandler}
import spray.http._
import spray.http.HttpHeaders.Location
import spray.util.LoggingContext
import java.util.UUID

import com.syntaxjockey.terane.indexer._

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
  implicit def supervisor: ActorRef
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout: Timeout

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
              supervisor.ask(createQuery).map {
                case CreatedQuery(op, SearchRef(_, search)) =>
                  HttpResponse(StatusCodes.Created,
                    JsonBody(search.toJson),
                    List(Location("http://%s:%d/1/queries/%s".format(hostname, settings.port, search.id))))
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

  /**
   * Spray routes for manipulating sinks (create, describe, delete)
   */
  val sourceId = Segment
  val sourcesRoutes = {
    path("1" / "sources") {
      pathEndOrSingleSlash {
        get {
          complete {
            supervisor.ask(EnumerateSources).map {
              case EnumeratedSources(sources) =>
                sources.map(_.source)
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }.mapTo[Seq[Source]]
          }
        } ~
        post {
          hostName { hostname =>
            entity(as[CreateSource]) { case createSource: CreateSource =>
              complete {
                supervisor.ask(createSource).map {
                  case CreatedSource(op, SourceRef(_, source)) =>
                    HttpResponse(StatusCodes.Created,
                      JsonBody(source.toJson),
                      List(Location("http://%s:%d/1/sources/%s".format(hostname, settings.port, source.settings.name))))
                  case failure: ApiFailure =>
                    throw new ApiException(failure)
                }.mapTo[HttpResponse]
              }
            }
          }
        }
      }
    } ~
    pathPrefix("1" / "sources" / sourceId) { case id: String =>
      pathEndOrSingleSlash {
        get {
          complete {
            supervisor.ask(DescribeSource(id)).map {
              case SourceRef(_, source) =>
                source
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }.mapTo[Source]
          }
        } ~
        delete {
          complete {
            supervisor.ask(DeleteSource(id)).map {
              case failure: ApiFailure =>
                throw new ApiException(failure)
              case _ => StatusCodes.OK
            }.mapTo[HttpResponse]
          }
        }
      }
    }
  }

  /**
   * Spray routes for manipulating sinks (create, describe, delete)
   */
  val sinkId = Segment
  val sinksRoutes = {
    path("1" / "sinks") {
      pathEndOrSingleSlash {
        get {
          complete {
            supervisor.ask(EnumerateSinks).map {
              case EnumeratedSinks(sinks) =>
                sinks.map(_.sink)
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }.mapTo[Seq[Sink]]
          }
        } ~
        post {
          hostName { hostname =>
            entity(as[CreateSink]) { case createSink: CreateSink =>
              complete {
                supervisor.ask(createSink).map {
                  case CreatedSink(op, SinkRef(_, sink)) =>
                    HttpResponse(StatusCodes.Created,
                      JsonBody(sink.toJson),
                      List(Location("http://%s:%d/1/sinks/%s".format(hostname, settings.port, sink.settings.name))))
                  case failure: ApiFailure =>
                    throw new ApiException(failure)
                }.mapTo[HttpResponse]
              }
            }
          }
        }
      }
    } ~
    pathPrefix("1" / "sinks" / sinkId) { case id: String =>
      pathEndOrSingleSlash {
        get {
          complete {
            supervisor.ask(DescribeSink(id)).map {
              case SinkRef(_, sink) =>
                sink
              case failure: ApiFailure =>
                throw new ApiException(failure)
            }.mapTo[Sink]
          }
        } ~
        delete {
          complete {
            supervisor.ask(DeleteSink(id)).map {
              case failure: ApiFailure =>
                throw new ApiException(failure)
              case _ => StatusCodes.OK
            }.mapTo[HttpResponse]
          }
        }
      }
    }
  }

  val version1 = queriesRoutes ~ sourcesRoutes ~ sinksRoutes

  val routes =  version1

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

  def throwableToJson(t: Throwable): JsValue = JsObject(Map("description" -> JsString(t.getMessage)))
}
