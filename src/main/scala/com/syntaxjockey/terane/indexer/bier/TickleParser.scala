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
import org.xbill.DNS.{Name, Address => DNSAddress}

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.{EveryMatcher, OrMatcher, AndMatcher, NotMatcher}

/**
 * Tickle EBNF Grammar
 */
trait TickleParser extends JavaTokenParsers {
  import TickleParser._

  /*
   * <RawValue>     ::= <RawText> | <RawLiteral> | <RawFloat> | <RawInteger> | <RawDatetime> | <RawHostname>
   * <RawText>      ::=
   * <RawLiteral>   ::=
   * <RawInteger>   ::=
   * <RawFloat>     ::=
   * <RawDatetime>  ::=
   * <RawHostname>  ::=
   */

  /* raw and shorthand types */
  val bareText: Parser[TargetText] = ident ^^ { TargetText }
  val quotedText: Parser[TargetText] = stringLiteral ^^ { value => TargetText(value.tail.init) }
  val rawText: Parser[TargetValue] = log(bareText | quotedText)("rawText")

  val rawLiteral: Parser[TargetValue] = log(regex("""'\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*""".r))("rawLiteral") ^^ { value => TargetLiteral(value.tail) }

  val rawNumber: Parser[TargetValue] = log(decimalNumber | wholeNumber)("rawNumber") ^^ {
    case number if number.contains('.') => TargetFloat(number)
    case number => TargetInteger(number)
  }

  /* see http://stackoverflow.com/questions/3143070/javascript-regex-iso-datetime*/
  val rawDatetime: Parser[TargetValue] = log(regex("""\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+(Z|(\+-)\d\d:\d\d)""".r))("rawDatetime") ^^ { TargetDatetime }

  val rawLocation: Parser[Any] = log(regex("""@\S+""".r))("rawLocation") ^^ {
    case value: String =>
      val location = value.tail
      // try to parse as an IPv4 or IPv6 address
      try {
        val address = DNSAddress.getByAddress(location)
        TargetAddress(location)
      } catch {
        case ex: Exception =>
          // try to parse as a URI
          // try to parse as a DNS name
          try {
            val hostname = Name.fromString(location)
            TargetHostname(location)
          } catch {
            case ex: Exception =>
            failure("don't know how to parse location '%s'".format(location))
          }
      }
      // try to parse as a file system path
  }

  /* raw value is a value which needs no coercion (its type is unambiguous) */
  val rawValue: Parser[TargetValue] = (rawLocation | rawDatetime | rawNumber | rawLiteral | rawText) ^^ {
    case value: TargetValue => value
  }

  /*
   * <CoercedValue>     ::= <CoercerFunction> '(' <CoercedString> ')'
   * <CoercerFunction>  ::= 'text' | 'literal' | 'integer' | 'float' | 'datetime' | 'address' | 'hostname'
   * <CoercedString>    ::= Regex(Sequence of any character except ASCII control, and ')' must be escaped with a backslash)
   */

  /* a coercion function and value parameter */
  val coercedValue: Parser[TargetValue] = regex("""(text|literal|integer|float|datetime|address|hostname)""".r) ~ regex("""\(([^\p{Cntrl}\\]|\\\)])*\)""".r) ^^ {
    case functionName ~ functionValue =>
      val value = functionValue.tail.init
      functionName match {
        case "text" => TargetText(value)
        case "literal" => TargetLiteral(value)
        case "integer" => TargetInteger(value)
        case "float" => TargetFloat(value)
        case "datetime" => TargetDatetime(value)
        case "address" => TargetAddress(value)
        case "hostname" => TargetHostname(value)
      }
  }

  /*
   * <TargetValue>   ::= <CoercedValue> | <RawValue>
   * <RawValue>      ::= <RawText> | <RawLiteral> | <RawInteger> | <RawFloat> | <RawDatetime> | <RawHostname>
   */

  /* a raw or coerced value */
  val targetValue: Parser[TargetValue] = coercedValue | rawValue

  /*
   * <Expression>         ::= <TargetExpression | <BareTarget>
   * <BareTarget>         ::= <TargetValue>
   * <Subject>            ::= ':' <JavaIdentifier>
   * <JavaIdentifier>     ::= see http://docs.oracle.com/javase/specs/jls/se7/html/jls-3.html#jls-3.8
   * <TargetExpression>   ::= <Equals> | <NotEquals> | <GreaterThan> | <LessThan> | <GreaterThanEquals> | <LessThanEquals>
   * <Equals>             ::= <Subject> '=' <TargetValue>
   * <NotEquals>          ::= <Subject> '!=' <TargetValue>
   * <GreaterThan>        ::= <Subject> '>' <TargetValue>
   * <LessThan>           ::= <Subject> '<' <TargetValue>
   * <GreaterThanEquals>  ::= <Subject> '>=' <TargetValue>
   * <LessThanEquals>     ::= <Subject> '<=' <TargetValue>
   * <Function>           ::= <Subject> '->' <FunctionName> '(' [ <FunctionArg> ]* ')'
   * <FunctionName>       ::= <JavaIdentifier>
   * <FunctionArg>        ::= <TargetValue>
   */

  /* subject is a java token starting with a ':' */
  val subject: Parser[String] = log(regex(""":\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*""".r))("subject") ^^ { _.tail }

  /* bare target is just a value without field name or type */
  val bareTarget: Parser[Expression] = log(targetValue)("bareTarget") ^^ { case target => Expression(None, PredicateEquals(target)) }

  /* a range with a start and an end */
  val closedRange: Parser[TargetRange] = log(regex("[\\[{]".r) ~ targetValue ~ literal("TO") ~ targetValue ~ regex("[}\\]]".r))("targetRange") ^^ {
    case "[" ~ valueStart ~ "TO" ~ valueEnd ~ "]" if valueStart.dataType == valueEnd.dataType =>
      TargetRange(Some(valueStart), Some(valueEnd), valueStart.dataType, startExcl = false, endExcl = false)
    case "{" ~ valueStart ~ "TO" ~ valueEnd ~ "}" if valueStart.dataType == valueEnd.dataType =>
      TargetRange(Some(valueStart), Some(valueEnd), valueStart.dataType, startExcl = true, endExcl = true)
    case "[" ~ valueStart ~ "TO" ~ valueEnd ~ "}" if valueStart.dataType == valueEnd.dataType =>
      TargetRange(Some(valueStart), Some(valueEnd), valueStart.dataType, startExcl = false, endExcl = true)
    case "{" ~ valueStart ~ "TO" ~ valueEnd ~ "]" if valueStart.dataType == valueEnd.dataType =>
      TargetRange(Some(valueStart), Some(valueEnd), valueStart.dataType, startExcl = true, endExcl = false)
  }

  val targetRange: Parser[TargetRange] = closedRange

  /* target expression has a field name, operator, and value */
  def equals: Parser[Expression] = log(subject ~ literal("=") ~ (targetRange | targetValue))("equals") ^^ {
    case name ~ "=" ~ (value: TargetRange) => Expression(Some(name), PredicateEqualsRange(value))
    case name ~ "=" ~ (value: TargetValue) => Expression(Some(name), PredicateEquals(value))
  }
  def notEquals: Parser[Expression] = log(subject ~ literal("!=") ~ (targetRange | targetValue))("notEquals") ^^ {
    case name ~ "!=" ~ (value: TargetRange) => Expression(Some(name), PredicateNotEqualsRange(value))
    case name ~ "!=" ~ (value: TargetValue) => Expression(Some(name), PredicateNotEquals(value))
  }
  def greaterThan: Parser[Expression] = log(subject ~ literal(">") ~ targetValue)("greaterThan") ^^ {
    case name ~ ">" ~ value => Expression(Some(name), PredicateGreaterThan(value))
  }
  def lessThan: Parser[Expression] = log(subject ~ literal("<") ~ targetValue)("lessThan") ^^ {
    case name ~ "<" ~ value => Expression(Some(name), PredicateLessThan(value))
  }
  def greaterThanEquals: Parser[Expression] = log(subject ~ literal(">=") ~ targetValue)("greaterThanEquals") ^^ {
    case name ~ ">=" ~ value => Expression(Some(name), PredicateGreaterThanEqualTo(value))
  }
  def lessThanEquals: Parser[Expression] = log(subject ~ literal("<=") ~ targetValue)("lessThanEquals") ^^ {
    case name ~ "<=" ~ value => Expression(Some(name), PredicateLessThanEqualTo(value))
  }
  def function: Parser[Expression] = log(subject ~ literal("->") ~ ident ~ literal("(") ~ rep(targetValue) ~ literal(")"))("function") ^^ {
    case name ~ "->" ~ functionName ~ "(" ~ functionArgs ~ ")" => Expression(Some(name), PredicateFunction(functionName, functionArgs))
  }

  def targetExpression: Parser[Expression] = equals | notEquals | greaterThanEquals | lessThanEquals | greaterThan | lessThan | function

  /* either qualified or bare subject expression */
  val expression: Parser[ExpressionOrGroup] = log(targetExpression | bareTarget)("expression") ^^ (Left(_))


  /*
   * <Query>        ::= <OrOperator>
   * <OrOperator>   ::= <AndOperator> ('OR' <AndOperator>)*
   * <AndOperator>  ::= <NotOperator> ('AND' <NotOperator>)*
   * <NotOperator>  ::= ['NOT'] <NotOperator> | <Group>
   * <Group>        ::= '(' <OrOperator> ')' | <Expression>
   */

  def Group: Parser[ExpressionOrGroup] = log((literal("(") ~> OrOperator <~ literal(")")) | expression)("group") ^^ {
    case expr: ExpressionOrGroup => expr
  }

  def NotOperator: Parser[ExpressionOrGroup] = log(("NOT" ~ NotOperator) | Group)("notOperator") ^^ {
    case "NOT" ~ (not: ExpressionOrGroup) => Right(NotGroup(not))
    case group: ExpressionOrGroup => group
  }

  def AndOperator: Parser[ExpressionOrGroup] = log(NotOperator ~ rep("AND" ~ NotOperator))("andOperator") ^^ {
    case not1 ~ nots if nots.isEmpty =>
      not1
    case not1 ~ nots =>
      val children: List[ExpressionOrGroup] = nots map { not =>
        not match {
          case "AND" ~ expressionOrGroup => expressionOrGroup
        }
      }
      Right(AndGroup(not1 +: children))
  }

  def OrOperator: Parser[ExpressionOrGroup] = log(AndOperator ~ rep("OR" ~ AndOperator))("orOperator") ^^ {
    case and1 ~ ands if ands.isEmpty =>
      and1
    case and1 ~ ands =>
      val children: List[ExpressionOrGroup] = ands map { and =>
        and match {
          case "OR" ~ expressionOrGroup => expressionOrGroup
        }
      }
      Right(OrGroup(and1 +: children))
  }

  /* the entry point */
  val query: Parser[Query] = log(OrOperator)("query") ^^ { Query }
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

  /**
   * Parse an expression, returning a Matchers tree.
   *
   * @param expression
   * @param params
   * @return
   */
  def parseExpression(expression: Expression, params: TickleParserParams)(implicit factory: ActorRefFactory): Option[Matchers] = {
    expression.predicate match {
      case predicate: TypedPredicate =>
        predicate.dataType match {
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
      case predicate: PredicateFunction =>
        TickleFunctions.parsePredicateFunction(factory, expression.subject.get, predicate.name, predicate.args, params)
      case unknown =>
        throw new Exception("unknown predicate type " + unknown.toString)
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
      case Left(Expression(_subject, predicate)) =>
        sb.append(" " * indent)
        _subject match {
          case Some(name) =>
            sb.append(name + " ")
          case None =>
            sb.append("? ")
        }
        predicate match {
          case PredicateEquals(target) =>
            sb.append("= " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateNotEquals(target) =>
            sb.append("!= " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateGreaterThan(target) =>
            sb.append("> " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateLessThan(target) =>
            sb.append("< " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateGreaterThanEqualTo(target) =>
            sb.append(">= " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateLessThanEqualTo(target) =>
            sb.append("<= " + target.dataType.toString.toLowerCase + "(" + target.raw + ")\n")
          case PredicateEqualsRange(TargetRange(start, end, _, startExcl, endExcl)) =>
            sb.append("= " + (if (startExcl) "{" else "["))
            sb.append((if (start.isDefined) start.get.raw else " ") + " TO " + (if (end.isDefined) end.get.raw else " "))
            sb.append(if (endExcl) "}" else "]")
          case PredicateFunction(name, args) =>
            sb.append("-> " + name + "(" + args.map(_.raw).mkString(", ") + ")\n")
          case other => throw new Exception("don't know how to prettyPrint '%s'".format(other))
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

  case class TargetRange(start: Option[TargetValue], end: Option[TargetValue], dataType: DataType.DataType, startExcl: Boolean, endExcl: Boolean)

  sealed abstract class Predicate
  case class PredicateFunction(name: String, args: Seq[TargetValue]) extends Predicate

  sealed abstract class TypedPredicate(val dataType: DataType.DataType) extends Predicate
  case class PredicateEquals(target: TargetValue) extends TypedPredicate(target.dataType)
  case class PredicateNotEquals(target: TargetValue) extends TypedPredicate(target.dataType)
  case class PredicateGreaterThan(target: TargetValue) extends TypedPredicate(target.dataType)
  case class PredicateLessThan(target: TargetValue) extends TypedPredicate(target.dataType)
  case class PredicateGreaterThanEqualTo(target: TargetValue) extends TypedPredicate(target.dataType)
  case class PredicateLessThanEqualTo(target: TargetValue) extends TypedPredicate(target.dataType)

  case class PredicateEqualsRange(range: TargetRange) extends TypedPredicate(range.dataType)
  case class PredicateNotEqualsRange(range: TargetRange) extends TypedPredicate(range.dataType)

}

case class TickleParserParams(defaultField: String)