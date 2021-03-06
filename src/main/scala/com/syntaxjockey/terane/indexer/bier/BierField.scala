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
import scala.Some
import java.util.Date
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.TickleParser._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers._
import com.syntaxjockey.terane.indexer.bier.statistics.{Analytical, FieldStatistics}
import com.syntaxjockey.terane.indexer.bier.statistics.Analytical._

/**
 * Identifies a field by name and type.
 */
case class FieldIdentifier(fieldName: String, fieldType: DataType.Value)

object FieldIdentifier {
  import scala.util.{Try,Success,Failure}

  def fromSpec(spec: String): Try[FieldIdentifier] = try {
    val fieldParts = spec.split(':')
    val fieldType = fieldParts(0)
    val fieldName = fieldParts(1)
    Success(FieldIdentifier(fieldName, DataType.withName(fieldType.toUpperCase)))
  } catch {
    case ex: Throwable => Failure(ex)
  }
}

/**
 *
 */
abstract class BierField {

  def toMatchTerm(value: String): MatchTerm


  def parseRange(fieldId: FieldIdentifier, targetRange: TargetRange): Matchers = targetRange match {
    case TargetRange(Some(start: TargetValue), Some(end: TargetValue), _, startExcl, endExcl) =>
      RangeMatcher(fieldId, ClosedRangeSpec(toMatchTerm(start.raw), toMatchTerm(end.raw), startExcl, endExcl))
    case TargetRange(None, Some(end: TargetValue), _, startExcl, endExcl) =>
      RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(end.raw), endExcl))
    case TargetRange(Some(start: TargetValue), None, _, startExcl, endExcl) =>
      RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(start.raw), startExcl))
  }

//  def parseRange(fieldId: FieldIdentifier, targetRange: TargetRange): Matchers = targetRange match {
//    case TargetRange(Some(TargetLiteral(start)), Some(TargetLiteral(end)), DataType.LITERAL, startExcl, endExcl) =>
//      RangeMatcher(fieldId, ClosedRangeSpec(toMatchTerm(start), toMatchTerm(end), startExcl, endExcl))
//    case TargetRange(None, Some(TargetLiteral(end)), DataType.LITERAL, startExcl, endExcl) =>
//      RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(end), endExcl))
//    case TargetRange(Some(TargetLiteral(start)), None, DataType.LITERAL, startExcl, endExcl) =>
//      RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(start), startExcl))
//  }
}

object BierField {
  case class PostingMetadata(positions: Option[scala.collection.mutable.Set[Int]])
}

/**
 *
 */
case class ParsedValue[T](postings: Seq[(T,PostingMetadata)], statistics: FieldStatistics)

/**
 * Parses a TEXT field.  The tokenizer is pretty stupid and simply splits the input Text
 * value on runs of whitespace, then downcases each term.
 */
class TextField extends BierField {

  def tokenizeValue(text: Text): Seq[String] = text.underlying.split("""\s+""").map(normalizeTerm)

  def normalizeTerm(term: String): String = term.toLowerCase

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

  def toMatchTerm(text: String): MatchTerm = {
    MatchTerm(text = Some(normalizeTerm(text)))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.TEXT)
    expression.predicate match {
      case PredicateEquals(TargetText(target)) =>
        val phrase = tokenizeValue(Text(target)).map {
          case "_" =>
            TermPlaceholder
          case term =>
            TermMatcher(fieldId, MatchTerm(text = Some(term)))
        }
        PhraseMatcher(phrase)(factory)
      case other => throw new Exception("parse failure")
    }
  }
}
object TextField extends TextField

/**
 * Parses a LITERAL field.  The tokenizer passes the supplied input Literal along as-is
 * without any processing.  Useful for keyword fields where whitespace, capitalization, special
 * characters, etc. must all be preserved exactly.
 */
class LiteralField extends BierField {

  def tokenizeValue(literal: Literal): String = literal.underlying

  def parseValue(literal: Literal): ParsedValue[String] = {
    ParsedValue(Seq((literal.underlying, PostingMetadata(None))), FieldStatistics(Seq(literal.underlying)))
  }

  def toMatchTerm(literal: String): MatchTerm = {
    MatchTerm(literal = Some(tokenizeValue(Literal(literal))))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.LITERAL)
    expression.predicate match {
      case PredicateEquals(TargetLiteral(target)) =>
        TermMatcher(fieldId, toMatchTerm(target))
      case PredicateNotEquals(TargetLiteral(target)) =>
        NotMatcher(EveryMatcher(), TermMatcher(fieldId, toMatchTerm(target)))(factory)
      case PredicateEqualsRange(targetRange: TargetRange) =>
        parseRange(fieldId, targetRange)
      case PredicateNotEqualsRange(targetRange: TargetRange) =>
        NotMatcher(EveryMatcher(), parseRange(fieldId, targetRange))(factory)
      case PredicateLessThan(TargetLiteral(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = true))
      case PredicateLessThanEqualTo(TargetLiteral(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = false))
      case PredicateGreaterThan(TargetLiteral(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target),startExcl = true))
      case PredicateGreaterThanEqualTo(TargetLiteral(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = false))
      case other => throw new Exception("Literal field parse failure")
    }
  }
}
object LiteralField extends LiteralField

/**
 * Parses an INTEGER field.  The tokenizer passes the supplied input Integer (a 64-bit long)
 * along as-is without any processing.
 */
class IntegerField extends BierField {

  def tokenizeValue(integer: Integer): Long = integer.underlying

  def parseValue(integer: Integer): ParsedValue[Long] = {
    ParsedValue(Seq((integer.underlying, PostingMetadata(None))), FieldStatistics(Seq(integer.underlying)))
  }

  def toMatchTerm(integer: String): MatchTerm = {
    MatchTerm(integer = Some(tokenizeValue(Integer(integer))))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.INTEGER)
    expression.predicate match {
      case PredicateEquals(TargetInteger(target)) =>
        TermMatcher(fieldId, toMatchTerm(target))
      case PredicateNotEquals(TargetInteger(target)) =>
        NotMatcher(EveryMatcher(), TermMatcher(fieldId, toMatchTerm(target)))(factory)
      case PredicateEqualsRange(targetRange: TargetRange) =>
        parseRange(fieldId, targetRange)
      case PredicateNotEqualsRange(targetRange: TargetRange) =>
        NotMatcher(EveryMatcher(), parseRange(fieldId, targetRange))(factory)
      case PredicateLessThan(TargetInteger(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = true))
      case PredicateLessThanEqualTo(TargetInteger(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = false))
      case PredicateGreaterThan(TargetInteger(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target),startExcl = true))
      case PredicateGreaterThanEqualTo(TargetInteger(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = false))
      case other => throw new Exception("Integer field parse failure")
    }
  }
}
object IntegerField extends IntegerField

/**
 * Parses a FLOAT field.  The tokenizer passes the supplied input Float (a 64-bit double) along
 * as-is without any processing.
 */
class FloatField extends BierField {

  def tokenizeValue(float: Float): Double = float.underlying

  def parseValue(float: Float): ParsedValue[Double] = {
    ParsedValue(Seq((float.underlying, PostingMetadata(None))), FieldStatistics(Seq(float.underlying)))
  }

  def toMatchTerm(float: String): MatchTerm = {
    MatchTerm(float = Some(tokenizeValue(Float(float))))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.FLOAT)
    expression.predicate match {
      case PredicateEquals(TargetFloat(target)) =>
        TermMatcher(fieldId, toMatchTerm(target))
      case PredicateNotEquals(TargetInteger(target)) =>
        NotMatcher(EveryMatcher(), TermMatcher(fieldId, toMatchTerm(target)))(factory)
      case PredicateEqualsRange(targetRange: TargetRange) =>
        parseRange(fieldId, targetRange)
      case PredicateNotEqualsRange(targetRange: TargetRange) =>
        NotMatcher(EveryMatcher(), parseRange(fieldId, targetRange))(factory)
      case PredicateLessThan(TargetInteger(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = true))
      case PredicateLessThanEqualTo(TargetInteger(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = false))
      case PredicateGreaterThan(TargetInteger(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target),startExcl = true))
      case PredicateGreaterThanEqualTo(TargetInteger(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = false))
      case other => throw new Exception("Float field parse failure")
    }
  }
}
object FloatField extends FloatField

/**
 * Parses a DATETIME field.
 */
class DatetimeField extends BierField {

  def tokenizeValue(datetime: Datetime): Date = datetime.underlying.toDate

  def parseValue(datetime: Datetime): ParsedValue[Date] = {
    ParsedValue(Seq((datetime.underlying.toDate, PostingMetadata(None))), FieldStatistics(Seq(datetime.underlying.toDate)))
  }

  def parseDatetimeString(s: String): DateTime = {
    DateTime.parse(s)
  }

  def toMatchTerm(datetime: String): MatchTerm = {
    MatchTerm(datetime = Some(tokenizeValue(Datetime(parseDatetimeString(datetime)))))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.DATETIME)
    expression.predicate match {
      case PredicateEquals(TargetDatetime(target)) =>
        TermMatcher(fieldId, toMatchTerm(target))
      case PredicateNotEquals(TargetDatetime(target)) =>
        NotMatcher(EveryMatcher(), TermMatcher(fieldId, toMatchTerm(target)))(factory)
      case PredicateEqualsRange(targetRange: TargetRange) =>
        parseRange(fieldId, targetRange)
      case PredicateNotEqualsRange(targetRange: TargetRange) =>
        NotMatcher(EveryMatcher(), parseRange(fieldId, targetRange))(factory)
      case PredicateLessThan(TargetDatetime(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = true))
      case PredicateLessThanEqualTo(TargetDatetime(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = false))
      case PredicateGreaterThan(TargetDatetime(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = true))
      case PredicateGreaterThanEqualTo(TargetDatetime(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = false))
      case other => throw new Exception("Datetime field parse failure")
    }
  }
}
object DatetimeField extends DatetimeField

/**
 * Parses an ADDRESS field.
 */
class AddressField extends BierField {

  def tokenizeValue(address: Address): Array[Byte] = address.underlying.getAddress

  def parseValue(address: Address): ParsedValue[Array[Byte]] = {
    ParsedValue(Seq((address.underlying.getAddress, PostingMetadata(None))), FieldStatistics(Seq(address.underlying.getAddress)))
  }

  def parseAddressString(s: String): InetAddress = {
    DNSAddress.getByAddress(s)
  }

  def toMatchTerm(address: String): MatchTerm = {
    MatchTerm(address = Some(tokenizeValue(Address(parseAddressString(address)))))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.ADDRESS)
    expression.predicate match {
      case PredicateEquals(TargetAddress(target)) =>
        TermMatcher(fieldId, toMatchTerm(target))
      case PredicateNotEquals(TargetAddress(target)) =>
        NotMatcher(EveryMatcher(), TermMatcher(fieldId, toMatchTerm(target)))(factory)
      case PredicateEqualsRange(targetRange: TargetRange) =>
        parseRange(fieldId, targetRange)
      case PredicateNotEqualsRange(targetRange: TargetRange) =>
        NotMatcher(EveryMatcher(), parseRange(fieldId, targetRange))(factory)
      case PredicateLessThan(TargetAddress(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = true))
      case PredicateLessThanEqualTo(TargetAddress(target)) =>
        RangeMatcher(fieldId, LeftOpenRangeSpec(toMatchTerm(target), endExcl = false))
      case PredicateGreaterThan(TargetAddress(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = true))
      case PredicateGreaterThanEqualTo(TargetAddress(target)) =>
        RangeMatcher(fieldId, RightOpenRangeSpec(toMatchTerm(target), startExcl = false))
      case other => throw new Exception("Address field parse failure")
    }
  }
}
object AddressField extends AddressField

/**
 * Parses a HOSTNAME field.
 */
class HostnameField extends BierField {
  import scala.language.postfixOps

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

  def toMatchTerm(hostname: String): MatchTerm = {
    MatchTerm(hostname = Some(hostname))
  }

  def parseExpression(factory: ActorRefFactory, expression: Expression, params: TickleParserParams): Matchers = {
    val fieldId = FieldIdentifier(expression.subject.getOrElse(params.defaultField), DataType.HOSTNAME)
    expression.predicate match {
      case PredicateEquals(TargetHostname(target)) =>
        val parsed = parseValue(Hostname(parseHostnameString(target)))
        PhraseMatcher(parsed.postings.map { case (term,metadata) => TermMatcher(fieldId, toMatchTerm(term)) })(factory)
      case other => throw new Exception("Hostname field parse failure")
    }
  }
}
object HostnameField extends HostnameField
