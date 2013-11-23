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

package com.syntaxjockey.terane.indexer.bier

import com.netflix.astyanax.util.TimeUUIDUtils
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.datatypes._

class BierEvent(val id: UUID, val values: Map[FieldIdentifier,EventValue], val tags: Set[String]) {
  import BierEvent._

  def +(kv: KeyValue): BierEvent = {
    new BierEvent(id, values + kv, tags)
  }

  def ++(xs: Traversable[KeyValue]): BierEvent = {
    new BierEvent(id, values ++ xs, tags)
  }

  def -(key: FieldIdentifier): BierEvent = {
    new BierEvent(id, values - key, tags)
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append(id.toString + ":")
    for ((k,v) <- values) {
      if (v.text.isDefined)
        sb.append(" %s:text='%s'".format(k, v.text.get.underlying))
      if (v.literal.isDefined)
        sb.append(" %s:literal=%s".format(k, v.literal.get.underlying))
      if (v.integer.isDefined)
        sb.append(" %s:integer=%d".format(k, v.integer.get.underlying))
      if (v.float.isDefined)
        sb.append(" %s:float=%f".format(k, v.float.get.underlying))
      if (v.datetime.isDefined)
        sb.append(" %s:datetime=%s".format(k, v.datetime.get.underlying))
      if (v.address.isDefined)
        sb.append(" %s:address=%s".format(k, v.address.get.underlying))
      if (v.hostname.isDefined)
        sb.append(" %s:hostname=%s".format(k, v.hostname.get.underlying))
    }
    if (!tags.isEmpty) {
      sb.append(" tags: %s".format(tags.mkString(",")))
    }
    sb.mkString
  }
}

object BierEvent {
  import scala.language.implicitConversions

  def apply(uuid: Option[UUID] = None, values: Map[FieldIdentifier,EventValue] = Map.empty, tags: Set[String] = Set.empty): BierEvent = {
    if (uuid.isDefined)
      new BierEvent(uuid.get, values, tags)
    else
      new BierEvent(TimeUUIDUtils.getUniqueTimeUUIDinMicros, values, tags)
  }

  type KeyValue = (FieldIdentifier,EventValue)

  implicit def text2keyValue(kv: (String, Text)): KeyValue = (FieldIdentifier(kv._1, DataType.TEXT), EventValue(text = Some(kv._2)))
  implicit def literal2keyValue(kv: (String, Literal)): KeyValue = (FieldIdentifier(kv._1, DataType.LITERAL), EventValue(literal = Some(kv._2)))
  implicit def integer2keyValue(kv: (String, Integer)): KeyValue = (FieldIdentifier(kv._1, DataType.INTEGER), EventValue(integer = Some(kv._2)))
  implicit def float2keyValue(kv: (String, Float)): KeyValue = (FieldIdentifier(kv._1, DataType.FLOAT), EventValue(float = Some(kv._2)))
  implicit def datetime2keyValue(kv: (String, Datetime)): KeyValue = (FieldIdentifier(kv._1, DataType.DATETIME), EventValue(datetime = Some(kv._2)))
  implicit def address2keyValue(kv: (String, Address)): KeyValue = (FieldIdentifier(kv._1, DataType.ADDRESS), EventValue(address = Some(kv._2)))
  implicit def hostname2keyValue(kv: (String, Hostname)): KeyValue = (FieldIdentifier(kv._1, DataType.HOSTNAME), EventValue(hostname = Some(kv._2)))
}

case class EventValue(
  text: Option[Text] = None,
  literal: Option[Literal] = None,
  integer: Option[Integer] = None,
  float: Option[Float] = None,
  datetime: Option[Datetime] = None,
  address: Option[Address] = None,
  hostname: Option[Hostname] = None
  )