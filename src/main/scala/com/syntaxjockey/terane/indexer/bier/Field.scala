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

import akka.actor.ActorRefFactory
import org.joda.time.DateTime
import org.xbill.DNS.{Name, Address => DNSAddress}
import java.util.Date
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.matchers.{TermMatcher, AndMatcher}
import com.syntaxjockey.terane.indexer.bier.statistics.{Analytical, FieldStatistics}
import com.syntaxjockey.terane.indexer.bier.statistics.Analytical._

abstract class Field

object Field {
  case class PostingMetadata(positions: Option[scala.collection.mutable.Set[Int]])
}

case class ParsedValue[T](postings: Seq[(T,PostingMetadata)], statistics: FieldStatistics)

/**
 * Parses a TEXT field.  The tokenizer is pretty stupid and simply splits the input Text
 * value on runs of whitespace.
 */
class TextField extends Field {

  def tokenizeValue(text: Text): Seq[String] = text.underlying.toLowerCase.split("""\s+""")

  def parseValue(text: Text): ParsedValue[String] = {
    val terms = tokenizeValue(text)
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    val stats: Seq[Analytical] = (0 until terms.size) map { position =>
      val term = terms(position)
      val stat: Analytical = term
      val postingMetadata = positions.getOrElseUpdate(term, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
      stat
    }
    ParsedValue(positions.toMap.toSeq, FieldStatistics(stats))
  }

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, text: String): Matchers = {
    AndMatcher(tokenizeValue(Text(text)).map { case term => TermMatcher(fieldId, term) }.toList)(factory)
  }
}
object TextField extends TextField

/**
 * Parses a LITERAL field.  The tokenizer passes the supplied input Literal along as-is
 * without any processing.  Useful for keyword fields where whitespace, capitalization, special
 * characters, etc. must all be preserved exactly.
 */
class LiteralField extends Field {

  def tokenizeValue(literal: Literal): String = literal.underlying

  def parseValue(literal: Literal): ParsedValue[String] = {
    ParsedValue(Seq((literal.underlying, PostingMetadata(None))), FieldStatistics(Seq(literal.underlying)))
  }

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, literal: String): Matchers = {
    TermMatcher(fieldId, tokenizeValue(Literal(literal)))
  }
}
object LiteralField extends LiteralField

/**
 * Parses an INTEGER field.  The tokenizer passes the supplied input Integer (a 64-bit long)
 * along as-is without any processing.
 */
class IntegerField extends Field {

  def tokenizeValue(integer: Integer): Long = integer.underlying

  def parseValue(integer: Integer): ParsedValue[Long] = {
    ParsedValue(Seq((integer.underlying, PostingMetadata(None))), FieldStatistics(Seq(integer.underlying)))
  }

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, integer: String): Matchers = {
    TermMatcher(fieldId, tokenizeValue(Integer(integer.toLong)))
  }
}
object IntegerField extends IntegerField

/**
 * Parses a FLOAT field.  The tokenizer passes the supplied input Float (a 64-bit double) along
 * as-is without any processing.
 */
class FloatField extends Field {

  def tokenizeValue(float: Float): Double = float.underlying

  def parseValue(float: Float): ParsedValue[Double] = {
    ParsedValue(Seq((float.underlying, PostingMetadata(None))), FieldStatistics(Seq(float.underlying)))
  }

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, float: String): Matchers = {
    TermMatcher(fieldId, tokenizeValue(Float(float.toDouble)))
  }
}
object FloatField extends FloatField

/**
 * Parses a DATETIME field.
 */
class DatetimeField extends Field {

  def tokenizeValue(datetime: Datetime): Date = datetime.underlying.toDate

  def parseValue(datetime: Datetime): ParsedValue[Date] = {
    ParsedValue(Seq((datetime.underlying.toDate, PostingMetadata(None))), FieldStatistics(Seq(datetime.underlying.toDate)))
  }

  def parseDatetimeString(s: String): DateTime = {
    DateTime.parse(s)
  }

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, datetime: String): Matchers = {
    TermMatcher(fieldId, parseValue(Datetime(parseDatetimeString(datetime))))
  }
}
object DatetimeField extends DatetimeField

/**
 * Parses an ADDRESS field.
 */
class AddressField extends Field {

  def tokenizeValue(address: Address): Array[Byte] = address.underlying.getAddress

  def parseValue(address: Address): ParsedValue[Array[Byte]] = {
    ParsedValue(Seq((address.underlying.getAddress, PostingMetadata(None))), FieldStatistics(Seq(address.underlying.getAddress)))
  }

  def parseAddressString(s: String): InetAddress = {
    DNSAddress.getByAddress(s)
  }

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, address: String): Matchers = {
    TermMatcher(fieldId, parseValue(Address(parseAddressString(address))))
  }
}
object AddressField extends AddressField

/**
 * Parses a HOSTNAME field.
 */
class HostnameField extends Field {

  def parseValue(hostname: Hostname): ParsedValue[String] = {
    val positions = new scala.collection.mutable.HashMap[String,PostingMetadata]()
    val nlabels = hostname.underlying.labels
    val stats: Seq[Analytical] = (0 until nlabels reverse) map { position =>
      val label = hostname.underlying.getLabelString(position).toLowerCase
      val stat: Analytical = label
      val postingMetadata = positions.getOrElseUpdate(label, PostingMetadata(Some(new scala.collection.mutable.HashSet[Int])))
      postingMetadata.positions.get += position
      stat
    }
    ParsedValue(positions.toMap.toSeq, FieldStatistics(stats))
  }

  def parseHostnameString(s: String): Name = Name.fromString(s)

  def makeMatcher(factory: ActorRefFactory, fieldId: FieldIdentifier, hostname: String): Matchers = {
    val parsed = parseValue(Hostname(parseHostnameString(hostname)))
    AndMatcher(parsed.postings.map { case (term,metadata) => TermMatcher(fieldId, term) }.toList)(factory)
  }
}
object HostnameField extends HostnameField
