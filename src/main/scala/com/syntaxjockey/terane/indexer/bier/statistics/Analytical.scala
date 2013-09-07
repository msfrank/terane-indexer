package com.syntaxjockey.terane.indexer.bier.statistics

import java.nio.{ByteOrder, ByteBuffer}
import java.nio.charset.Charset
import java.util.Date

/**
 * classes which implement the Analytical trait must provide three different views which
 * uniquely identify the class value *within its particular namespace*.
 */
trait Analytical {
  def hash: Long
  def bytes: Array[Byte]
  def string: String
}

object Analytical {
  import scala.language.implicitConversions

  val CHARSET_LATIN1 = Charset.forName("ISO-8859-1")

  implicit def string2AnalyticalValue(string: String): Analytical = {
    AnalyticalValue(MurmurHash.murmurHash128(string)._1, string.getBytes, string)
  }

  implicit def long2AnalyticalValue(long: Long): Analytical = {
    AnalyticalValue(long, long.toString.getBytes, long.toString)
  }

  implicit def double2AnalyticalValue(double: Double): Analytical = {
    val buffer: Array[Byte] = new Array(8)
    val bb = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN)
    bb.putDouble(double)
    AnalyticalValue(bb.getLong(0), buffer, new String(buffer, CHARSET_LATIN1))
  }

  implicit def date2AnalyticalValue(date: Date): Analytical = {
    val buffer: Array[Byte] = new Array(8)
    ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN).putLong(date.getTime)
    AnalyticalValue(date.getTime, buffer, new String(buffer, CHARSET_LATIN1))
  }

  implicit def bytesAnalyticalValue(bytes: Array[Byte]): Analytical = {
    // mapping the byte array to a String with a latin-1 charset is a sneaky way to map a sequence
    // of 8-bit values to a String without translation.
    AnalyticalValue(MurmurHash.murmurHash128(bytes)._1, bytes, new String(bytes, CHARSET_LATIN1))
  }
}

case class AnalyticalValue(hash: Long, bytes: Array[Byte], string: String) extends Analytical
