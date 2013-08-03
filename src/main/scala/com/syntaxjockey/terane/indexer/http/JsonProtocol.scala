package com.syntaxjockey.terane.indexer.http

import spray.json._
import org.joda.time.DateTime
import java.util.UUID

import com.syntaxjockey.terane.indexer.EventRouter
import com.syntaxjockey.terane.indexer.bier.{EventValueType, Event}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{CreatedQuery, CreateQuery}
import com.syntaxjockey.terane.indexer.sink.Query.{GetEvents, QueryStatistics}
import com.syntaxjockey.terane.indexer.sink.Query.EventsBatch

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
        FieldIdentifier(fieldName, EventValueType.withName(fieldTypeName))
    }
  }

  /* convert Event class */
  implicit object EventFormat extends RootJsonFormat[Event] {
    def write(event: Event) = {
      val fields: List[JsField] = for ((FieldIdentifier(name, _), value) <- event.values.toList) yield {
        val values = scala.collection.mutable.HashMap[String,JsValue]()
        for (text <- value.text)
          values(EventValueType.TEXT.toString) = JsString(text)
        for (literal <- value.literal)
          values(EventValueType.LITERAL.toString) = JsArray(literal.map(JsString(_)))
        for (integer <- value.integer)
          values(EventValueType.INTEGER.toString) = JsNumber(integer)
        for (float <- value.float)
          values(EventValueType.FLOAT.toString) = JsNumber(float)
        for (datetime <- value.datetime)
          values(EventValueType.DATETIME.toString) = JsNumber(datetime.getMillis)
        for (address <- value.address)
          values(EventValueType.ADDRESS.toString) = JsString(address.getHostAddress)
        for (hostname <- value.hostname)
          values(EventValueType.HOSTNAME.toString) = JsString(hostname.toString)
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
  //implicit val ListQueriesResponseFormat = jsonFormat1(ListQueriesResponse.apply)
  implicit val CreateQueryFormat = jsonFormat6(CreateQuery.apply)
  implicit val CreatedQueryFormat = jsonFormat1(CreatedQuery.apply)
  implicit val QueryStatisticsFormat = jsonFormat5(QueryStatistics.apply)
  implicit val GetEventsFormat = jsonFormat1(GetEvents.apply)
  implicit val EventsBatchFormat = jsonFormat3(EventsBatch.apply)
}
