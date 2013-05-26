package com.syntaxjockey.terane.indexer.bier

import java.util.UUID
import org.joda.time.DateTime
import java.net.InetAddress
import scala.collection.mutable.MapProxy
import scala.collection.mutable

/**
 *
 */
class Event(val id: UUID) extends MapProxy[String,Event.Value] {

  val self = new mutable.HashMap[String,Event.Value]()

  def set(name: String, text: Event.Text): Event = {
    val value = getOrElse(name, Event.Value()).copy(text = Some(text))
    this.+=((name, value))
  }

  def set(name: String, literal: Event.Literal): Event = {
    val value = getOrElse(name, Event.Value()).copy(literal = Some(literal))
    this.+=((name, value))
  }

  def set(name: String, integer: Event.Integer): Event = {
    val value = getOrElse(name, Event.Value()).copy(integer = Some(integer))
    this.+=((name, value))
  }

  def set(name: String, float: Event.Float): Event = {
    val value = getOrElse(name, Event.Value()).copy(float = Some(float))
    this.+=((name, value))
  }

  def set(name: String, datetime: Event.Datetime): Event = {
    val value = getOrElse(name, Event.Value()).copy(datetime = Some(datetime))
    this.+=((name, value))
  }

  def set(name: String, address: Event.Address): Event = {
    val value = getOrElse(name, Event.Value()).copy(address = Some(address))
    this.+=((name, value))
  }

  def set(name: String, hostname: Event.Hostname): Event = {
    val value = getOrElse(name, Event.Value()).copy(hostname = Some(hostname))
    this.+=((name, value))
  }

  override def toString(): String = {
    val sb = new StringBuilder()
    sb.append(id.toString + ":")
    for ((k,v) <- this) {
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

object Event {
  type Text = String
  type Literal = Set[String]
  type Integer = Long
  type Float = Double
  type Datetime = DateTime
  type Address = InetAddress
  type Hostname = List[String]

  case class Value(
    text: Option[Text] = None,
    literal: Option[Literal] = None,
    integer: Option[Integer] = None,
    float: Option[Float] = None,
    datetime: Option[Datetime] = None,
    address: Option[Address] = None,
    hostname: Option[Hostname] = None)

  def apply(uuid: Option[UUID] = None): Event = {
    if (uuid.isDefined) new Event(uuid.get) else new Event(UUID.randomUUID())
  }
}
