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

package com.syntaxjockey.terane.indexer.bier.datatypes

import org.joda.time.DateTime
import org.xbill.DNS.Name
import java.net.InetAddress
import java.util.Date

object DataType extends Enumeration {
  type DataType = Value
  val TEXT, LITERAL, INTEGER, FLOAT, DATETIME, ADDRESS, HOSTNAME = Value
}

sealed trait DataValue extends Any

class Text(val underlying: String) extends AnyVal with Comparable[Text] with DataValue {
  def compareTo(other: Text) = underlying.compareTo(other.underlying)
}

object Text {
  def apply(string: String) = new Text(string)
}

class Literal(val underlying: String) extends AnyVal with Comparable[Literal] with DataValue {
  def compareTo(other: Literal) = underlying.compareTo(other.underlying)
}

object Literal {
  def apply(string: String) = new Literal(string)
}

class Integer(val underlying: Long) extends AnyVal with Comparable[Integer] with DataValue {
  def compareTo(other: Integer) = underlying.compareTo(other.underlying)
}

object Integer {
  def apply(long: scala.Long) = new Integer(long)
  def apply(int: scala.Int) = new Integer(int.toLong)
  def apply(string: String) = new Integer(string.toLong)
}

class Float(val underlying: Double) extends AnyVal with Comparable[Float] with DataValue {
  def compareTo(other: Float) = underlying.compareTo(other.underlying)
}

object Float {
  def apply(double: scala.Double) = new Float(double)
  def apply(float: scala.Float) = new Float(float.toDouble)
  def apply(string: String) = new Float(string.toDouble)
}

class Datetime(val underlying: DateTime) extends AnyVal with Comparable[Datetime] with DataValue {
  def compareTo(other: Datetime) = underlying.compareTo(other.underlying)
}

object Datetime {
  def apply(date: Date) = new Datetime(new DateTime(date))
  def apply(datetime: DateTime) = new Datetime(datetime)
  def apply(string: String) = new Datetime(new DateTime(string.toLong))
}

class Address(val underlying: InetAddress) extends AnyVal with Comparable[Address] with DataValue {
  def compareTo(other: Address): Int = {
    var a1 = underlying.getAddress
    var a2 = other.underlying.getAddress
    if (a1.length != a2.length) {
      a1 = if (a1.length == 6) Address.ipv4prefix ++ a1 else a1
      a2 = if (a2.length == 6) Address.ipv4prefix ++ a2 else a2
    }
    compareSameRank(a1, a2)
  }
  def compareSameRank(a1: Array[Byte], a2: Array[Byte]): Int = {
    0.until(a1.length).foreach { i =>
      val v1 = a1(i)
      val v2 = a2(i)
      if (v1 < v2)
        return -1
      if (v1 > v2)
        return 1
    }
    0
  }
}

object Address {
  val ipv4prefix: Array[Byte] = Array(
    0.toByte, 0.toByte, 0.toByte, 0.toByte,
    0.toByte, 0.toByte, 0.toByte, 0.toByte,
    0.toByte, 0.toByte, 0xff.toByte, 0xff.toByte
  )
  def apply(bytes: Array[Byte]) = new Address(InetAddress.getByAddress(bytes))
  def apply(inetaddress: InetAddress) = new Address(inetaddress)
  def apply(string: String) = new Address(org.xbill.DNS.Address.getByAddress(string))
}

class Hostname(val underlying: Name) extends AnyVal with Comparable[Hostname] with DataValue {
  def compareTo(other: Hostname) = underlying.compareTo(other.underlying)
}

object Hostname {
  def apply(name: Name) = new Hostname(name)
  def apply(string: String) = new Hostname(Name.fromString(string))
}
