/*
 * Copyright (c) 2010-2014 Michael Frank <msfrank@syntaxjockey.com>
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
 *
 */

package com.syntaxjockey.terane.indexer.sink

import org.joda.time.{DateTimeZone, DateTime, Duration => JodaDuration}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldMap

/**
 * Contains common functionality used by both SortingStreamer and DirectStreamer,
 * such as mapping field identifiers to their integer index and their compact string
 * encoding.
 */
abstract class Streamer(val created: DateTime, val fields: FieldMap) {

  private val base64keys: Map[Int,String] = Map(
    0x00 -> "a", 0x01 -> "b", 0x02 -> "c", 0x03 -> "d", 0x04 -> "e", 0x05 -> "f", 0x06 -> "g", 0x07 -> "h",
    0x08 -> "i", 0x09 -> "j", 0x0a -> "k", 0x0b -> "l", 0x0c -> "m", 0x0d -> "n", 0x0e -> "o", 0x0f -> "p",
    0x10 -> "q", 0x11 -> "r", 0x12 -> "s", 0x13 -> "t", 0x14 -> "u", 0x15 -> "v", 0x16 -> "w", 0x17 -> "x",
    0x18 -> "y", 0x19 -> "z", 0x1a -> "A", 0x1b -> "B", 0x1c -> "C", 0x1d -> "D", 0x1e -> "E", 0x1f -> "F",
    0x20 -> "G", 0x21 -> "H", 0x22 -> "I", 0x23 -> "J", 0x24 -> "K", 0x25 -> "L", 0x26 -> "M", 0x27 -> "N",
    0x28 -> "O", 0x29 -> "P", 0x2a -> "Q", 0x2b -> "R", 0x2c -> "S", 0x2d -> "T", 0x2e -> "U", 0x2f -> "V",
    0x30 -> "W", 0x31 -> "X", 0x32 -> "Y", 0x33 -> "Z", 0x34 -> "0", 0x35 -> "1", 0x36 -> "2", 0x37 -> "3",
    0x38 -> "4", 0x39 -> "5", 0x3a -> "6", 0x3b -> "7", 0x3c -> "8", 0x3d -> "9", 0x3e -> "+", 0x3f -> "/"
  )

  val ident2index: Map[FieldIdentifier,Int] = fields.fieldsByIdent.keys.zip(0 to fields.fieldsByIdent.size).toMap
  val index2ident: Map[Int,FieldIdentifier] = ident2index.map(e => e._2 -> e._1)

  /**
   * convert an integer index value to a string key.  note that the string is not
   * Base64 compatible, it just shares the same character set.
   */
  def index2key(index: Int): String = {
    // split index into 6 hextets
    val hextets: Seq[Int] = for (i <- 0.until(6).reverse) yield (index >> (i * 6)) & 0x3f
    // strip off any leading zeroes, map each hextet to a char, concatenate into a string
    val key = hextets.foldLeft("") {
      case (string, hextet) if string == "" && hextet == 0 =>
        string
      case (string, hextet) =>
        string + base64keys(hextet)
    }
    if (key == "") base64keys(0) else key
  }

  def getFieldIndex(fieldId: FieldIdentifier): Option[Int] = ident2index.get(fieldId)

  def getFieldId(index: Int): Option[FieldIdentifier] = index2ident.get(index)

  def getRuntime: Duration = Duration(new JodaDuration(created, DateTime.now(DateTimeZone.UTC)).getMillis, TimeUnit.MILLISECONDS)
}
