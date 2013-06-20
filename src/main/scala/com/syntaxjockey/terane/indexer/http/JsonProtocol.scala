package com.syntaxjockey.terane.indexer.http

import com.syntaxjockey.terane.indexer.EventRouter
import spray.json._
import java.util.UUID
import com.syntaxjockey.terane.indexer.bier.Event
import org.joda.time.DateTime

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

  /* convert Event class */
  implicit object EventFormat extends RootJsonFormat[Event] {
    def write(event: Event) = {
      val fields: List[JsField] = for ((name, value) <- event.toList) yield {
        val values = scala.collection.mutable.HashMap[String,JsValue]()
        for (text <- value.text)
          values("text") = JsString(text)
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
  implicit val CreateQueryFormat = jsonFormat5(CreateQuery.apply)
  implicit val CreateQueryResponseFormat = jsonFormat1(CreateQueryResponse.apply)
  implicit val DescribeQueryResponseFormat = jsonFormat3(DescribeQueryResponse.apply)
  implicit val DeleteQueryFormat = jsonFormat1(DeleteQuery.apply)

}
