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
import scala.util.parsing.combinator.JavaTokenParsers

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.{EveryMatcher, OrMatcher, AndMatcher, NotMatcher}

/**
 * Tickle EBNF Grammar is as follows:
 *
 * SubjectTerm ::= identifier
 * Subject     ::= <subjectTerm>
 * NotGroup    ::= [ 'NOT' ] <IterSubject> | '(' <IterOrGroup> ')'
 * AndGroup    ::= <IterSubject>  [ 'AND' <IterNotGroup> ]*
 * OrGroup     ::= <IterAndGroup> [ 'OR' <IterAndGroup> ]*
 * Expr        ::= ( 'ALL' | <IterOrGroup> ) [ 'WHERE' <subjectDate> ]
 */
trait TickleParser extends JavaTokenParsers {
  import TickleParser._

  /* raw and shorthand types */
  val rawText: Parser[String] = ident | stringLiteral

  val rawLiteral: Parser[String] = """'\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*""".r ^^ { _.tail }

  val rawInteger: Parser[String] = wholeNumber

  val rawFloat: Parser[String] = decimalNumber | floatingPointNumber

  /* see http://stackoverflow.com/questions/3143070/javascript-regex-iso-datetime*/
  val rawDatetime: Parser[String] = """\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+""".r ^^ { _.toString }

  /* see http://answers.oreilly.com/topic/318-how-to-match-ipv4-addresses-with-regular-expressions/ */
  //val rawIpv4address: Parser[String] = """^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$""".r ^^ { _.toString }

  /* see http://stackoverflow.com/questions/53497/regular-expression-that-matches-valid-ipv6-addresses */
  //val rawIpv6address: Parser[String] = """(?>(?>([a-f0-9]{1,4})(?>:(?1)){7}|(?!(?:.*[a-f0-9](?>:|$)){8,})((?1)(?>:(?1)){0,6})?::(?2)?)|(?>(?>(?1)(?>:(?1)){5}:|(?!(?:.*[a-f0-9]:){6,})(?3)?::(?>((?1)(?>:(?1)){0,4}):)?)?(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])(?>\.(?4)){3}))""".r ^^ { _.toString }

  /* see http://stackoverflow.com/questions/106179/regular-expression-to-match-hostname-or-ip-address */
  val rawHostname: Parser[String] = """@(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])""".r ^^ { _.tail }

  /* raw value is a value which needs no coercion (its type is unambiguous) */
  val rawValue: Parser[TargetValue] = rawText ^^ { TargetText }
                                      rawLiteral ^^ { TargetLiteral } |
                                      rawInteger ^^ { TargetInteger } |
                                      rawFloat ^^ { TargetFloat } |
                                      rawDatetime ^^ { TargetDatetime } |
                                      rawHostname ^^ { TargetHostname }

  /* a coercion function and value parameter */
  val CoercionFunction = """(text|literal|integer|float|datetime|address|hostname)\((([^\p{Cntrl}\\]|\\\)])*)\)""".r
  val coercedValue: Parser[TargetValue] = CoercionFunction ^^ {
    case CoercionFunction(functionName, value) => functionName match {
      case "text" => TargetText(value)
      case "literal" => TargetLiteral(value)
      case "integer" => TargetInteger(value)
      case "float" => TargetFloat(value)
      case "datetime" => TargetDatetime(value)
      case "address" => TargetAddress(value)
      case "hostname" => TargetHostname(value)
    }
  }

  /* a raw or coerced value */
  val targetValue: Parser[TargetValue] = coercedValue | rawValue

  /* bare target is just a value without field name or type */
  val bareTarget: Parser[Expression] = targetValue ^^ { case target => Expression(None, PredicateEquals(target)) }

  /* target expression has a field name, operator, and value */
  def targetExpression: Parser[Expression] = elem(':') ~ ident ~ elem('=') ~ targetValue ^^ {
    case ':' ~ name ~ '=' ~ value => Expression(Some(name), PredicateEquals(value))
  } | ':' ~ ident ~ ("!=" ~ targetValue) ^^ {
    case ':' ~ name ~ ("!=" ~ value) => Expression(Some(name), PredicateNotEquals(value))
  }

  /* either qualified or bare subject expression */
  val expression: Parser[ExpressionOrGroup] = targetExpression ^^ (Left(_)) | bareTarget ^^ (Left(_))

  /* match a NOT group */
  def notGroup: Parser[ExpressionOrGroup] = "NOT" ~> expression ^^ {
    case s => Right(NotGroup(s))
  } | elem('(') ~> orGroup <~ elem(')') ^^ {
    case or => or
  } | expression ^^ { s: ExpressionOrGroup => s }
//  def notGroup: Parser[ExpressionOrGroup] = "NOT" ~ expression ^^ {
//    case "NOT" ~ s => Right(NotGroup(s))
//  } | "(" ~ orGroup ~ ")" ^^ {
//    case "(" ~ or ~ ")" => or
//  } | expression ^^ { s: ExpressionOrGroup => s }

  /* match an AND group */
  def andGroup: Parser[ExpressionOrGroup] = expression ~ rep1("AND" ~ notGroup) ^^ {
    case s ~ nots =>
      val children: List[ExpressionOrGroup] = nots map { not =>
        not match {
          case "AND" ~ expressionOrGroup => expressionOrGroup
        }
      }
      Right(AndGroup(s +: children))
  } | elem('(') ~ expression ~ rep1("AND" ~ notGroup) ~ elem(')') ^^ {
    case '(' ~ s ~ nots ~ ')' =>
      val children: List[ExpressionOrGroup] = nots map { not =>
        not match {
          case "AND" ~ expressionOrGroup => expressionOrGroup
        }
      }
      Right(AndGroup(s +: children))
  } | expression ^^ { s: ExpressionOrGroup => s }

  /* match an OR group */
  def orGroup: Parser[ExpressionOrGroup] = andGroup ~ rep1("OR" ~ andGroup) ^^ {
    case and1 ~ ands =>
      val children = ands map { and =>
        and match {
          case "OR" ~ expressionOrGroup => expressionOrGroup
        }
      }
      Right(OrGroup(and1 +: children))
  } | elem('(') ~ andGroup ~ rep1("OR" ~ andGroup) ~ elem(')') ^^ {
    case '(' ~ and1 ~ ands ~ ')' =>
      val children = ands map { and =>
        and match {
          case "OR" ~ expressionOrGroup => expressionOrGroup
        }
      }
      Right(OrGroup(and1 +: children))
  } | andGroup ^^ { and: ExpressionOrGroup => and }

  /* the entry point */
  val query: Parser[Query] = orGroup ^^ {
    case expressionOrGroup => Query(expressionOrGroup)
  } | notGroup ^^ {
    case expressionOrGroup => Query(expressionOrGroup)
  }
}

object TickleParser extends TickleParser {
  import scala.language.postfixOps

  /**
   * Given a raw query string, produce a syntax tree.
   * @param qs
   * @return
   */
  def parseQueryString(qs: String): Query = {
    val result = parseAll(query, qs)
    if (!result.successful)
      throw new Exception("parsing was unsuccessful")
    result.get
  }

  /**
   * Given a raw query string, produce a Matchers tree.  An implicit ActorRefFactory
   * is expected to be in scope, because some Matchers may need to use Actors for processing
   * results.
   *
   * @param qs
   * @return
   */
  def buildMatchers(qs: String, params: TickleParserParams)(implicit factory: ActorRefFactory): Option[Matchers] = {
    parseExpressionOrGroup(parseQueryString(qs).query, params)
  }

  /**
   * Recursively descend the syntax tree and build a Matchers tree.  An implicit
   * ActorRefFactory is expected to be in scope, because some Matchers may need to use
   * Actors for processing results.
   *
   * @param expressionOrGroup
   * @return
   */
  def parseExpressionOrGroup(expressionOrGroup: ExpressionOrGroup, params: TickleParserParams)(implicit factory: ActorRefFactory): Option[Matchers] = {
    val lifted = liftMatchers(expressionOrGroup match {
      case Left(expr: Expression) =>
        parseExpression(expr, params)
      case Right(AndGroup(children)) =>
        val andMatcher = new AndMatcher(children.map { child => parseExpressionOrGroup(child, params) }.flatten.toSet)
        if (andMatcher.children.isEmpty) None else Some(andMatcher)
      case Right(OrGroup(children)) =>
        val orMatcher = new OrMatcher(children.map { child => parseExpressionOrGroup(child, params) }.flatten.toSet)
        if (orMatcher.children.isEmpty) None else Some(orMatcher)
      case Right(NotGroup(child)) =>
        val childMatcher = parseExpressionOrGroup(child, params)
        if (childMatcher.isEmpty) None else Some(new NotMatcher(new EveryMatcher(), childMatcher.get))
      case Right(unknown) =>
        throw new Exception("unknown group type " + unknown.toString)
    })
    siftMatchers(lifted)
  }

  def parseExpression(expression: Expression, params: TickleParserParams)(implicit factory: ActorRefFactory): Option[Matchers] = {
    expression.predicate.dataType match {
      case DataType.TEXT =>
        Some(TextField.parseExpression(factory, expression, params))
      case DataType.LITERAL =>
        Some(LiteralField.parseExpression(factory, expression, params))
      case DataType.INTEGER =>
        Some(IntegerField.parseExpression(factory, expression, params))
      case DataType.FLOAT =>
        Some(FloatField.parseExpression(factory, expression, params))
      case DataType.DATETIME =>
        Some(DatetimeField.parseExpression(factory, expression, params))
      case DataType.HOSTNAME =>
        Some(HostnameField.parseExpression(factory, expression, params))
      case DataType.ADDRESS =>
        Some(AddressField.parseExpression(factory, expression, params))
    }
  }
  /**
   * Pull up the subtree if the group only has one matcher.  An implicit ActorRefFactory
   * is expected to be in scope, because some Matchers may need to use Actors for processing
   * results.
   *
   * @param matchers
   * @return
   */
  def liftMatchers(matchers: Option[Matchers])(implicit factory: ActorRefFactory): Option[Matchers] = {
    matchers match {
      case andMatcher @ Some(AndMatcher(children)) =>
        if (children.isEmpty)
          None
        else if (children.size == 1) Some(children.head) else andMatcher
      case orMatcher @ Some(OrMatcher(children)) =>
        if (children.isEmpty)
          None
        else if (children.size == 1) Some(children.head) else orMatcher
      case other: Some[Matchers] =>
        other
      case None =>
        None
    }
  }

  /**
   * Reorder the subtree by separating additive matchers (AND, OR) from subtractive matchers (NOT).
   */
  def siftMatchers(matchers: Option[Matchers])(implicit factory: ActorRefFactory): Option[Matchers] = {
    matchers match {
      case andMatcher @ Some(AndMatcher(children)) =>
        val (additive: List[Matchers], subtractive: List[Matchers]) = children.toList.partition(m => !m.isInstanceOf[NotMatcher])
        // reorder AndMatcher if both additive and subtractive children are present
        if (additive.length > 0 && subtractive.length > 0) {
          val source = additive.toSet ++ subtractive.map { case m: NotMatcher => m.source }
          val filter = subtractive.map { case m: NotMatcher => m.filter }.toSet
          // possibly remove redundant EveryMatcher from the source
          val reduced = source - EveryMatcher()
          if (reduced.size > 1)
            Some(NotMatcher(AndMatcher(reduced), OrMatcher(filter)))
          else if (reduced.size == 1)
            Some(NotMatcher(reduced.head, OrMatcher(filter)))
          else
            Some(NotMatcher(EveryMatcher(), OrMatcher(filter)))
        } else andMatcher
      case other: Some[Matchers] =>
        other
      case None =>
        None
    }
  }

  /**
   * Return the string representation of a query syntax tree.
   */
  def prettyPrint(query: Query): String = {
    prettyPrintImpl(new StringBuilder(), query.query, 0).mkString
  }
  private def prettyPrintImpl(sb: StringBuilder, expressionOrGroup: ExpressionOrGroup, indent: Int): StringBuilder = {
    expressionOrGroup match {
      case Left(Expression(subject, predicate)) =>
        sb.append(" " * indent)
        subject match {
          case Some(_subject) =>
            sb.append(_subject + " ")
          case None =>
            sb.append("? ")
        }
        predicate match {
          case PredicateEquals(target) =>
            sb.append("= " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateNotEquals(target) =>
            sb.append("!= " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case other => throw new Exception("parse failure")
        }
      case Right(AndGroup(children)) =>
        sb.append(" " * indent)
        sb.append("AND\n")
        children.foreach(prettyPrintImpl(sb, _, indent + 2))
      case Right(OrGroup(children)) =>
        sb.append(" " * indent)
        sb.append("OR\n")
        children.foreach(prettyPrintImpl(sb, _, indent + 2))
      case Right(NotGroup(child)) =>
        sb.append(" " * indent)
        sb.append("NOT\n")
        prettyPrintImpl(sb, child, indent + 2)
      case other =>
        sb.append(" " * indent)
        sb.append(other.toString)
    }
    sb
  }

  type ExpressionOrGroup = Either[Expression,Group]

  case class Query(query: Either[Expression,Group])

  sealed abstract class Group
  case class AndGroup(children: List[ExpressionOrGroup]) extends Group
  case class OrGroup(children: List[ExpressionOrGroup]) extends Group
  case class NotGroup(child: ExpressionOrGroup) extends Group

  case class Expression(subject: Option[String], predicate: Predicate)

  sealed abstract class TargetValue(val raw: String, val dataType: DataType.DataType)
  case class TargetText(text: String) extends TargetValue(text, DataType.TEXT)
  case class TargetLiteral(literal: String) extends TargetValue(literal, DataType.LITERAL)
  case class TargetInteger(integer: String) extends TargetValue(integer, DataType.INTEGER)
  case class TargetFloat(float: String) extends TargetValue(float, DataType.FLOAT)
  case class TargetDatetime(datetime: String) extends TargetValue(datetime, DataType.DATETIME)
  case class TargetAddress(address: String) extends TargetValue(address, DataType.ADDRESS)
  case class TargetHostname(hostname: String) extends TargetValue(hostname, DataType.HOSTNAME)

  sealed abstract class Predicate(val dataType: DataType.DataType)
  case class PredicateEquals(target: TargetValue) extends Predicate(target.dataType)
  case class PredicateNotEquals(target: TargetValue) extends Predicate(target.dataType)
  case class PredicateGreaterThan(target: TargetValue) extends Predicate(target.dataType)
  case class PredicateLessThan(target: TargetValue) extends Predicate(target.dataType)
  case class PredicateGreaterThanEqualTo(target: TargetValue) extends Predicate(target.dataType)
  case class PredicateLessThanEqualTo(target: TargetValue) extends Predicate(target.dataType)
  case class PredicateEqualsRange(start: TargetValue, end: TargetValue, startExcl: Boolean, endExcl: Boolean)
  case class PredicateEqualsNotRange(start: TargetValue, end: TargetValue, startExcl: Boolean, endExcl: Boolean)
}

case class TickleParserParams(defaultField: String)