package com.syntaxjockey.terane.indexer.bier

import scala.util.parsing.combinator.syntactical._

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
      Subject(s.value, Some(fieldName), Some(fieldType))
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
  } | subject ^^ { s: SubjectOrGroup => s }

  /* match an OR group */
  val orGroup: Parser[SubjectOrGroup] = andGroup ~ rep1("OR" ~ andGroup) ^^ {
    case and1 ~ ands =>
      val children = ands map { and =>
        and match {
          case "AND" ~ subjectOrGroup => subjectOrGroup
        }
      }
      Right(OrGroup(and1 +: children))
  } | andGroup ^^ { and: SubjectOrGroup => and }

  /* the entry point */
  val query: Parser[Query] = orGroup ^^ (Query(_))

  /**
   *
   */
  def parseAll[T](p: Parser[T], in: String): ParseResult[T] =
    phrase(p)(new lexical.Scanner(in))
}

object TickleParser {

  private val parser = new TickleParser()

  def parseQueryString(qs: String): Query = {
    val result = parser.parseAll(parser.query, qs)
    if (!result.successful)
      throw new Exception("parsing was unsuccessful")
    result.get
  }

  type SubjectOrGroup = Either[Subject,Group]

  abstract class Group
  case class AndGroup(children: List[SubjectOrGroup]) extends Group
  case class OrGroup(children: List[SubjectOrGroup]) extends Group
  case class NotGroup(children: List[SubjectOrGroup]) extends Group
  case class Query(query: Either[Subject,Group])
  case class Subject(value: String, fieldName: Option[String], fieldType: Option[String])
}
