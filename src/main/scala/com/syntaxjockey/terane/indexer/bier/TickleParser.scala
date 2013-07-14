package com.syntaxjockey.terane.indexer.bier

import scala.util.parsing.combinator.syntactical._
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.matchers.{OrMatcher, AndMatcher, TermMatcher}
import java.util.Date
import org.xbill.DNS.Name

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
        Some(EventValueType.withName(fieldType.toUpperCase))
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
  private val textParser = new TextField()
  private val literalParser = new LiteralField()
  private val integerParser = new IntegerField()
  private val floatParser = new FloatField()
  private val datetimeParser = new DatetimeField()
  private val addressParser = new AddressField()
  private val hostnameParser = new HostnameField()


  /**
   * Given a raw query string, produce a Matchers tree.
   *
   * @param qs
   * @return
   */
  def buildMatchers(qs: String): Option[Matchers] = {
    parseSubjectOrGroup(parseQueryString(qs).query)
  }

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
   * Recursively descend the syntax tree and build a Matchers tree.
   *
   * @param subjectOrGroup
   * @return
   */
  def parseSubjectOrGroup(subjectOrGroup: SubjectOrGroup): Option[Matchers] = {
    subjectOrGroup match {
      case Left(Subject(value, fieldName, fieldType)) =>
        val fieldId = FieldIdentifier(fieldName.getOrElse("message"), fieldType.getOrElse(EventValueType.TEXT))
        val terms: List[Matchers] = fieldId.fieldType match {
          case EventValueType.TEXT =>
            textParser.makeValue(value).map(v => new TermMatcher[String](fieldId, v._1)).toList
          case EventValueType.LITERAL =>
            literalParser.makeValue(value).map(v => new TermMatcher[String](fieldId, v._1)).toList
          case EventValueType.INTEGER =>
            integerParser.makeValue(value).map(v => new TermMatcher[Long](fieldId, v._1)).toList
          case EventValueType.FLOAT =>
            floatParser.makeValue(value).map(v => new TermMatcher[Double](fieldId, v._1)).toList
          case EventValueType.DATETIME =>
            datetimeParser.makeValue(value).map(v => new TermMatcher[Date](fieldId, v._1)).toList
          case EventValueType.HOSTNAME =>
            hostnameParser.makeValue(value).map(v => new TermMatcher[String](fieldId, v._1)).toList
          case EventValueType.ADDRESS =>
            addressParser.makeValue(value).map(v => new TermMatcher[Array[Byte]](fieldId, v._1)).toList
          case unknown =>
            throw new Exception("unknown value type " + unknown.toString)
        }
        Some(new AndMatcher(terms))
      // FIXME: parse all value types
      case Right(AndGroup(children)) =>
        val andMatcher = new AndMatcher(children map { child => parseSubjectOrGroup(child) } flatten)
        if (andMatcher.children.isEmpty) None else Some(andMatcher)
      case Right(OrGroup(children)) =>
        val orMatcher = new OrMatcher(children map { child => parseSubjectOrGroup(child) } flatten)
        if (orMatcher.children.isEmpty) None else Some(orMatcher)
      case Right(unknown) =>
        throw new Exception("unknown group type " + unknown.toString)
    }
  }

  type SubjectOrGroup = Either[Subject,Group]

  abstract class Group
  case class AndGroup(children: List[SubjectOrGroup]) extends Group
  case class OrGroup(children: List[SubjectOrGroup]) extends Group
  case class NotGroup(children: List[SubjectOrGroup]) extends Group
  case class Query(query: Either[Subject,Group])
  case class Subject(value: String, fieldName: Option[String], fieldType: Option[EventValueType.Value])
}
