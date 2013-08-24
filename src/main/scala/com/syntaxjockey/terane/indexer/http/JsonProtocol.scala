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
import java.nio.charset.Charset
import java.util.UUID

import com.syntaxjockey.terane.indexer.EventRouter
import com.syntaxjockey.terane.indexer.bier.datatypes.DataType
import com.syntaxjockey.terane.indexer.bier.Event
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{CreatedQuery, CreateQuery}
import com.syntaxjockey.terane.indexer.sink.Query.{GetEvents, QueryStatistics, EventSet}

object JsonProtocol extends DefaultJsonProtocol {
  import EventRouter._

  /* convert UUID class */
  implicit object UUIDFormat extends RootJsonFormat[UUID] {
    def write(uuid: UUID) = JsString(uuid.toString)
    def read(value: JsValue) = value match {
      case JsString(uuid) => UUID.fromString(uuid)
      case _ => throw new DeserializationException("expected UUID")
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
  implicit object EventFormat extends RootJsonFormat[Event] {
    def write(event: Event) = {
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

  /* convert DateTime class */
  implicit object DateTimeFormat extends RootJsonFormat[DateTime] {
    def write(datetime: DateTime) = JsString(datetime.getMillis.toString)
    def read(value: JsValue) = value match {
      case JsString(datetime) => new DateTime(datetime.toLong)
      case _ => throw new DeserializationException("expected DateTime")
    }
  }

  /* convert query case classes */
  implicit val CreateQueryFormat = jsonFormat6(CreateQuery.apply)
  implicit val CreatedQueryFormat = jsonFormat1(CreatedQuery.apply)
  implicit val QueryStatisticsFormat = jsonFormat5(QueryStatistics.apply)
  implicit val GetEventsFormat = jsonFormat2(GetEvents.apply)
  implicit val EventSetFormat = jsonFormat2(EventSet.apply)
}

object JsonBody {
  val charset = Charset.defaultCharset()
  def apply(js: JsValue): HttpEntity = HttpEntity(ContentTypes.`application/json`, js.prettyPrint.getBytes(charset))
}