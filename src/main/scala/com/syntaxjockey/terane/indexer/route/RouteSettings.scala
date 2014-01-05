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

package com.syntaxjockey.terane.indexer.route

import spray.json._
import scala.Some

object RouteSettings extends DefaultJsonProtocol {

  implicit object MatchStatementFormat extends RootJsonFormat[MatchStatement] {
    def write(matchStatement: MatchStatement) = matchStatement match {
      case MatchesAll =>
        JsObject("matchType" -> JsString("matches-all"))
      case MatchesNone =>
        JsObject("matchType" -> JsString("matches-none"))
      case MatchesTag(tag) =>
        JsObject("matchType" -> JsString("matches-tag"), "tag" -> JsString(tag))
      case unknown => throw new SerializationException("don't know how to serialize %s".format(unknown))
    }
    def read(value: JsValue) = value match {
      case obj: JsObject =>
        obj.fields.get("matchType") match {
          case Some(matchType) =>
            matchType match {
              case JsString("matches-all") =>
                MatchesAll
              case JsString("matches-none") =>
                MatchesNone
              case JsString("match-tag") =>
                obj.fields.get("tag") match {
                  case Some(JsString(tag)) =>
                    MatchesTag(tag)
                  case None =>
                    throw new DeserializationException("MatchesTag is missing tag")
                  case unknown =>
                    throw new DeserializationException("MatchesTag contains unexpected value '%s' for tag".format(unknown))
                }
              case unknown =>
                throw new DeserializationException("unknown matchType '%s'".format(unknown))
            }
          case None =>
            throw new DeserializationException("MatchStatement is missing matchType")
        }

      case _ => throw new DeserializationException("expected MatchStatement")
    }
  }

  implicit object MatchActionFormat extends RootJsonFormat[MatchAction] {
    def write(matchAction: MatchAction) = matchAction match {
      case DropAction =>
        JsObject("actionType" -> JsString("drop-action"))
      case StoreAction(targets) =>
        JsObject("actionType" -> JsString("store-action"), "targets" -> JsArray(targets.map(JsString(_)).toList))
      case StoreAllAction =>
        JsObject("actionType" -> JsString("store-all-action"))
      case unknown => throw new SerializationException("don't know how to serialize %s".format(unknown))
    }
    def read(value: JsValue) = value match {
      case obj: JsObject =>
        obj.fields.get("actionType") match {
          case Some(matchType) =>
            matchType match {
              case JsString("drop-action") =>
                DropAction
              case JsString("store-action") =>
                obj.fields.get("targets") match {
                  case Some(JsArray(targets)) =>
                    StoreAction(targets.map {
                      case JsString(target) => target
                      case unknown => throw new DeserializationException("store-action targets must be strings")
                    }.toVector)
                  case None =>
                    throw new DeserializationException("StoreAction is missing targets")
                  case unknown =>
                    throw new DeserializationException("StoreAction contains unexpected value '%s' for targets".format(unknown))
                }
              case JsString("store-all-action") =>
                StoreAllAction
              case unknown =>
                throw new DeserializationException("unknown actionType '%s'".format(unknown))
            }
          case None =>
            throw new DeserializationException("MatchStatement is missing actionType")
        }

      case _ => throw new DeserializationException("expected MatchAction")
    }
  }

  implicit val RouteContextFormat = jsonFormat3(RouteContext.apply)
}