package com.syntaxjockey.terane.indexer.bier

import org.joda.time.DateTime
import com.netflix.astyanax.util.TimeUUIDUtils
import org.xbill.DNS.Name
import java.net.InetAddress
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.Event.Value
import com.syntaxjockey.terane.indexer.bier.Event.KeyValue

class Event(val id: UUID, val values: Map[FieldIdentifier,Value]) {

  def +(kv: KeyValue): Event = {
    new Event(id, values + kv)
  }

  def ++(xs: Traversable[KeyValue]): Event = {
    new Event(id, values ++ xs)
  }

  def -(key: FieldIdentifier): Event = {
    new Event(id, values - key)
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append(id.toString + ":")
    for ((k,v) <- values) {
      if (v.text.isDefined)
        sb.append(" %s:text='%s'".format(k, v.text.get))
      if (v.literal.isDefined)
        sb.append(" %s:literal=%s".format(k, v.literal.get))
      if (v.integer.isDefined)
        sb.append(" %s:integer=%d".format(k, v.integer.get))
      if (v.float.isDefined)
        sb.append(" %s:float=%f".format(k, v.float.get))
      if (v.datetime.isDefined)
        sb.append(" %s:datetime=%s".format(k, v.datetime.get))
      if (v.address.isDefined)
        sb.append(" %s:address=%s".format(k, v.address.get))
      if (v.hostname.isDefined)
        sb.append(" %s:hostname=%s".format(k, v.hostname.get))
    }
    sb.mkString
  }
}

object EventValueType extends Enumeration {
  type EventValueType = Value
  val TEXT, LITERAL, INTEGER, FLOAT, DATETIME, ADDRESS, HOSTNAME = Value
}

object Event {
  type Text = String
  type Literal = List[String]
  type Integer = Long
  type Float = Double
  type Datetime = DateTime
  type Address = InetAddress
  type Hostname = Name

  case class Value(
    text: Option[Text] = None,
    literal: Option[Literal] = None,
    integer: Option[Integer] = None,
    float: Option[Float] = None,
    datetime: Option[Datetime] = None,
    address: Option[Address] = None,
    hostname: Option[Hostname] = None)

  def apply(uuid: Option[UUID] = None, values: Map[FieldIdentifier,Value] = Map.empty): Event = {
    if (uuid.isDefined)
      new Event(uuid.get, values)
    else
      new Event(TimeUUIDUtils.getUniqueTimeUUIDinMicros, values)
  }

  type KeyValue = (FieldIdentifier,Value)

  implicit def text2keyValue(kv: (String, Text)): KeyValue = (FieldIdentifier(kv._1, EventValueType.TEXT), Value(text = Some(kv._2)))
  implicit def literal2keyValue(kv: (String, Literal)): KeyValue = (FieldIdentifier(kv._1, EventValueType.LITERAL), Value(literal = Some(kv._2)))
  implicit def integer2keyValue(kv: (String, Integer)): KeyValue = (FieldIdentifier(kv._1, EventValueType.INTEGER), Value(integer = Some(kv._2)))
  implicit def float2keyValue(kv: (String, Float)): KeyValue = (FieldIdentifier(kv._1, EventValueType.FLOAT), Value(float = Some(kv._2)))
  implicit def datetime2keyValue(kv: (String, Datetime)): KeyValue = (FieldIdentifier(kv._1, EventValueType.DATETIME), Value(datetime = Some(kv._2)))
  implicit def address2keyValue(kv: (String, Address)): KeyValue = (FieldIdentifier(kv._1, EventValueType.ADDRESS), Value(address = Some(kv._2)))
  implicit def hostname2keyValue(kv: (String, Hostname)): KeyValue = (FieldIdentifier(kv._1, EventValueType.HOSTNAME), Value(hostname = Some(kv._2)))
}
