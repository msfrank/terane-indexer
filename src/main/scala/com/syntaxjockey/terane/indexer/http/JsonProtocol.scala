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

import spray.http.{ContentTypes, HttpEntity}
import spray.json._
import org.joda.time.DateTime
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.nio.charset.Charset
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.datatypes.DataType
import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{CreatedQuery, CreateQuery}
import com.syntaxjockey.terane.indexer.sink.Query.{GetEvents, QueryStatistics, EventSet}
import scala.util.parsing.json.JSONObject

object JsonProtocol extends DefaultJsonProtocol {

  /* convert UUID class */
  implicit object UUIDFormat extends RootJsonFormat[UUID] {
    def write(uuid: UUID) = JsString(uuid.toString)
    def read(value: JsValue) = value match {
      case JsString(uuid) => UUID.fromString(uuid)
      case _ => throw new DeserializationException("expected UUID")
    }
  }

  /* convert DateTime class */
  implicit object DateTimeFormat extends RootJsonFormat[DateTime] {
    def write(datetime: DateTime) = JsString(datetime.getMillis.toString)
    def read(value: JsValue) = value match {
      case JsString(datetime) => new DateTime(datetime.toLong)
      case _ => throw new DeserializationException("expected DateTime")
    }
  }

  /* convert Duration class */
  implicit object DurationFormat extends RootJsonFormat[Duration] {
    def write(duration: Duration) = JsNumber(duration.toMillis)
    def read(value: JsValue) = value match {
      case JsNumber(duration) => Duration(duration.toLong, TimeUnit.MILLISECONDS)
      case _ => throw new DeserializationException("expected Duration")
    }
  }

  /* convert FieldIdentifier class */
  implicit object FieldIdentifierFormat extends RootJsonFormat[FieldIdentifier] {
    def write(ident: FieldIdentifier) = JsArray(JsString(ident.fieldType.toString), JsString(ident.fieldName))
    def read(value: JsValue) = value match {
      case JsArray(List(JsString(fieldTypeName), JsString(fieldName))) =>
        FieldIdentifier(fieldName, DataType.withName(fieldTypeName.toUpperCase))
    }
  }

  /* convert Event class */
  implicit object EventFormat extends RootJsonFormat[BierEvent] {
    def write(event: BierEvent) = {
      val fields: List[JsField] = for ((FieldIdentifier(name, _), value) <- event.values.toList) yield {
        val values = scala.collection.mutable.HashMap[String,JsValue]()
        for (text <- value.text)
          values(DataType.TEXT.toString) = JsString(text.underlying)
        for (literal <- value.literal)
          values(DataType.LITERAL.toString) = JsString(literal.underlying)
        for (integer <- value.integer)
          values(DataType.INTEGER.toString) = JsNumber(integer.underlying)
        for (float <- value.float)
          values(DataType.FLOAT.toString) = JsNumber(float.underlying)
        for (datetime <- value.datetime)
          values(DataType.DATETIME.toString) = JsNumber(datetime.underlying.getMillis)
        for (address <- value.address)
          values(DataType.ADDRESS.toString) = JsString(address.underlying.getHostAddress)
        for (hostname <- value.hostname)
          values(DataType.HOSTNAME.toString) = JsString(hostname.underlying.toString)
        name -> JsObject(values.toMap)
      }
      JsArray(event.id.toJson, JsObject(fields))
    }
    def read(value: JsValue) = value match {
      case _ => throw new DeserializationException("don't know how to deserialize Event")
    }
  }

  /* convert EventSet class */
  implicit object EventSetFormat extends RootJsonFormat[EventSet] {
    def writeEvent(event: BierEvent, ident2key: Map[FieldIdentifier,String]): JsValue = {
      val fields: Map[String,JsValue] = event.values.map {
        case (fieldId @ FieldIdentifier(_, DataType.TEXT), value) =>
          ident2key(fieldId) -> JsString(value.text.get.underlying)
        case (fieldId @ FieldIdentifier(_, DataType.LITERAL), value) =>
          ident2key(fieldId) -> JsString(value.literal.get.underlying)
        case (fieldId @ FieldIdentifier(_, DataType.INTEGER), value) =>
          ident2key(fieldId) -> JsNumber(value.integer.get.underlying)
        case (fieldId @ FieldIdentifier(_, DataType.FLOAT), value) =>
          ident2key(fieldId) -> JsNumber(value.float.get.underlying)
        case (fieldId @ FieldIdentifier(_, DataType.DATETIME), value) =>
          ident2key(fieldId) -> JsNumber(value.datetime.get.underlying.getMillis)
        case (fieldId @ FieldIdentifier(_, DataType.ADDRESS), value) =>
          ident2key(fieldId) -> JsString(value.address.get.underlying.getHostAddress)
        case (fieldId @ FieldIdentifier(_, DataType.HOSTNAME), value) =>
          ident2key(fieldId) -> JsString(value.hostname.get.underlying.toString)
      }
      JsArray(event.id.toJson, JsObject(fields))
    }
    def write(eventSet: EventSet) = {
      val ident2key: Map[FieldIdentifier,String] = eventSet.fields.map(e => e._2 -> e._1)
      JsObject(Map(
        "fields" -> eventSet.fields.toJson,
        "events" -> JsArray(eventSet.events.map(writeEvent(_, ident2key))),
        "stats" -> eventSet.stats.toJson,
        "finished" -> eventSet.finished.toJson
      ))
    }
    def read(value: JsValue) = value match {
      case _ => throw new DeserializationException("don't know how to deserialize EventSet")
    }
  }

  /* convert CreateQuery class */
  implicit val CreateQueryFormat = jsonFormat6(CreateQuery.apply)

  /* convert CreatedQuery class */
  implicit val CreatedQueryFormat = jsonFormat1(CreatedQuery.apply)

  /* convert QueryStatistics class */
  implicit val QueryStatisticsFormat = jsonFormat6(QueryStatistics.apply)

  /* convert GetEvents class */
  implicit val GetEventsFormat = jsonFormat2(GetEvents.apply)
}

object JsonBody {
  val charset = Charset.defaultCharset()
  def apply(js: JsValue): HttpEntity = HttpEntity(ContentTypes.`application/json`, js.prettyPrint.getBytes(charset))
}