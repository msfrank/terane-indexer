package com.syntaxjockey.terane.indexer.bier.datatypes

import org.joda.time.DateTime
import org.xbill.DNS.Name
import java.net.InetAddress

object DataType extends Enumeration {
  type DataType = Value
  val TEXT, LITERAL, INTEGER, FLOAT, DATETIME, ADDRESS, HOSTNAME = Value
}

class Text(val underlying: String) extends AnyVal with Comparable[Text] {
  def compareTo(other: Text) = underlying.compareTo(other.underlying)
}

object Text {
  def apply(string: String) = new Text(string)
}

class Literal(val underlying: String) extends AnyVal with Comparable[Literal] {
  def compareTo(other: Literal) = underlying.compareTo(other.underlying)
}

object Literal {
  def apply(string: String) = new Literal(string)
}

class Integer(val underlying: Long) extends AnyVal with Comparable[Integer] {
  def compareTo(other: Integer) = underlying.compareTo(other.underlying)
}

object Integer {
  def apply(long: scala.Long) = new Integer(long)
  def apply(int: scala.Int) = new Integer(int.toLong)
}

class Float(val underlying: Double) extends AnyVal with Comparable[Float] {
  def compareTo(other: Float) = underlying.compareTo(other.underlying)
}

object Float {
  def apply(double: scala.Double) = new Float(double)
  def apply(float: scala.Float) = new Float(float.toDouble)
}

class Datetime(val underlying: DateTime) extends AnyVal with Comparable[Datetime] {
  def compareTo(other: Datetime) = underlying.compareTo(other.underlying)
}

object Datetime {
  def apply(datetime: DateTime) = new Datetime(datetime)
}

class Address(val underlying: InetAddress) extends AnyVal with Comparable[Address] {
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
  def apply(inetaddress: InetAddress) = new Address(inetaddress)
  def apply(string: String) = new Address(org.xbill.DNS.Address.getByAddress(string))
}

class Hostname(val underlying: Name) extends AnyVal with Comparable[Hostname] {
  def compareTo(other: Hostname) = underlying.compareTo(other.underlying)
}

object Hostname {
  def apply(name: Name) = new Hostname(name)
  def apply(string: String) = new Hostname(Name.fromString(string))
}
