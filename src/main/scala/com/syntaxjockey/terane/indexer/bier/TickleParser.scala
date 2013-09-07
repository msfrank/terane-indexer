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
import scala.util.parsing.combinator.syntactical._

import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.{OrMatcher, AndMatcher}

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
class TickleParser extends StandardTokenParsers {

  /* get our case classes */
  import TickleParser._

  lexical.delimiters += ( "=", "(", ")", "[", "]" )
  lexical.reserved += ( "AND", "OR", "NOT" )

  /* basic subject without field name or type */
  val bareSubject: Parser[Subject] = ident ^^ (Subject(_, None, None)) | stringLit ^^ (Subject(_, None, None)) | numericLit ^^ (Subject(_, None, None))

  /* subject with field name, and optional type */
  val qualifiedSubject: Parser[Subject] = ident ~ opt("[" ~ ident ~ "]") ~ "=" ~ bareSubject ^^ {
    case fieldName ~ Some("[" ~ fieldType ~ "]") ~ "=" ~ s =>
      val fieldValueType = try {
        Some(DataType.withName(fieldType.toUpperCase))
      } catch { case ex: Exception => None }
      if (fieldValueType.isEmpty)
        failure("unknown field type " + fieldType)
      Subject(s.value, Some(fieldName), fieldValueType)
    case fieldName ~ None ~ "=" ~ s =>
      Subject(s.value, Some(fieldName), None)
  }

  /* either qualified or bare subject */
  val subject: Parser[SubjectOrGroup] = qualifiedSubject ^^ (Left(_)) | bareSubject ^^ (Left(_))

  /* match a NOT group */
  val notGroup: Parser[SubjectOrGroup] = "NOT" ~ subject ^^ {
    case "NOT" ~ s => Right(NotGroup(List(s)))
  } | "(" ~ orGroup ~ ")" ^^ {
    case "(" ~ or ~ ")" => or
  } | subject ^^ { s: SubjectOrGroup => s }

  /* match an AND group */
  val andGroup: Parser[SubjectOrGroup] = subject ~ rep1("AND" ~ notGroup) ^^ {
    case s ~ nots =>
      val children: List[SubjectOrGroup] = nots map { not =>
        not match {
          case "AND" ~ subjectOrGroup => subjectOrGroup
        }
      }
      Right(AndGroup(s +: children))
  } | "(" ~ subject ~ rep1("AND" ~ notGroup) ~ ")" ^^ {
    case "(" ~ s ~ nots ~ ")" =>
      val children: List[SubjectOrGroup] = nots map { not =>
        not match {
          case "AND" ~ subjectOrGroup => subjectOrGroup
        }
      }
      Right(AndGroup(s +: children))
  } | subject ^^ { s: SubjectOrGroup => s }

  /* match an OR group */
  val orGroup: Parser[SubjectOrGroup] = andGroup ~ rep1("OR" ~ andGroup) ^^ {
    case and1 ~ ands =>
      val children = ands map { and =>
        and match {
          case "OR" ~ subjectOrGroup => subjectOrGroup
        }
      }
      Right(OrGroup(and1 +: children))
  } | "(" ~ andGroup ~ rep1("OR" ~ andGroup) ~ ")" ^^ {
    case "(" ~ and1 ~ ands ~ ")" =>
      val children = ands map { and =>
        and match {
          case "OR" ~ subjectOrGroup => subjectOrGroup
        }
      }
      Right(OrGroup(and1 +: children))
  } | andGroup ^^ { and: SubjectOrGroup => and }

  /* the entry point */
  val query: Parser[Query] = orGroup ^^ {
    case subjectOrGroup => Query(subjectOrGroup)
  } | notGroup ^^ {
    case subjectOrGroup => Query(subjectOrGroup)
  }

  /**
   *
   */
  def parseAll[T](p: Parser[T], in: String): ParseResult[T] =
    phrase(p)(new lexical.Scanner(in))
}

object TickleParser {
  import scala.language.postfixOps

  private val parser = new TickleParser()

  /**
   * Given a raw query string, produce a syntax tree.
   * @param qs
   * @return
   */
  def parseQueryString(qs: String): Query = {
    val result = parser.parseAll(parser.query, qs)
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
  def buildMatchers(qs: String)(implicit factory: ActorRefFactory): Option[Matchers] = {
    parseSubjectOrGroup(parseQueryString(qs).query)
  }

  /**
   * Recursively descend the syntax tree and build a Matchers tree.  An implicit
   * ActorRefFactory is expected to be in scope, because some Matchers may need to use
   * Actors for processing results.
   *
   * @param subjectOrGroup
   * @return
   */
  def parseSubjectOrGroup(subjectOrGroup: SubjectOrGroup)(implicit factory: ActorRefFactory): Option[Matchers] = {
    liftMatchers(subjectOrGroup match {
      case Left(Subject(value, fieldName, fieldType)) =>
        val fieldId = FieldIdentifier(fieldName.getOrElse("message"), fieldType.getOrElse(DataType.TEXT))
        Some(fieldId.fieldType match {
          case DataType.TEXT =>
            TextField.makeMatcher(factory, fieldId, value)
          case DataType.LITERAL =>
            LiteralField.makeMatcher(factory, fieldId, value)
          case DataType.INTEGER =>
            IntegerField.makeMatcher(factory, fieldId, value)
          case DataType.FLOAT =>
            FloatField.makeMatcher(factory, fieldId, value)
          case DataType.DATETIME =>
            DatetimeField.makeMatcher(factory, fieldId, value)
          case DataType.HOSTNAME =>
            HostnameField.makeMatcher(factory, fieldId, value)
          case DataType.ADDRESS =>
            AddressField.makeMatcher(factory, fieldId, value)
          case unknown =>
            throw new Exception("unknown value type " + unknown.toString)
        })
      case Right(AndGroup(children)) =>
        val andMatcher = new AndMatcher(children map { child => parseSubjectOrGroup(child) } flatten)
        if (andMatcher.children.isEmpty) None else Some(andMatcher)
      case Right(OrGroup(children)) =>
        val orMatcher = new OrMatcher(children map { child => parseSubjectOrGroup(child) } flatten)
        if (orMatcher.children.isEmpty) None else Some(orMatcher)
      case Right(unknown) =>
        throw new Exception("unknown group type " + unknown.toString)
    })
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
        else if (children.length == 1) Some(children.head) else andMatcher
      case orMatcher @ Some(OrMatcher(children)) =>
        if (children.isEmpty)
          None
        else if (children.length == 1) Some(children.head) else orMatcher
      case other: Some[Matchers] =>
        other
      case None =>
        None
    }
  }

  type SubjectOrGroup = Either[Subject,Group]

  abstract class Group
  case class AndGroup(children: List[SubjectOrGroup]) extends Group
  case class OrGroup(children: List[SubjectOrGroup]) extends Group
  case class NotGroup(children: List[SubjectOrGroup]) extends Group
  case class Query(query: Either[Subject,Group])
  case class Subject(value: String, fieldName: Option[String], fieldType: Option[DataType.Value])
}
