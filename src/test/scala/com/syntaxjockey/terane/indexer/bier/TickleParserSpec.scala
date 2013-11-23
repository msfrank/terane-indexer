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

import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpec
import org.scalatest.Inside._
import org.slf4j.LoggerFactory
import org.joda.time.{DateTimeZone, DateTime}
import org.xbill.DNS.{Address => DNSAddress}
import scala.Some
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.TickleParser._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.{TermPlaceholder, PhraseMatcher, AndMatcher, TermMatcher}
import com.syntaxjockey.terane.indexer.TestCluster

class TickleParserSpec extends TestCluster("TickleParserSpec") with WordSpec with MustMatchers {

  val logger = LoggerFactory.getLogger(classOf[TickleParserSpec])

  val params = TickleParserParams("fieldname")

  "TickleParser.parseQueryString()" must {

    "parse a bare text predicate" in {
      val query = TickleParser.parseQueryString("foobar")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(None, PredicateEquals(TargetText("foobar"))))
        )
      )
    }

    "parse a bare integer predicate" in {
      val query = TickleParser.parseQueryString("42")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(None, PredicateEquals(TargetInteger("42"))))
        )
      )
    }

    "parse a text predicate with a field name" in {
      val query = TickleParser.parseQueryString("?fieldname = foobar")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEquals(TargetText("foobar"))))
        )
      )
    }

    "parse a bare quoted text predicate" in {
      val query = TickleParser.parseQueryString(""" "hello, world!" """)
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(None, PredicateEquals(TargetText("hello, world!"))))
        )
      )
    }

    "parse a quoted text subject with a field name" in {
      val query = TickleParser.parseQueryString(""" ?fieldname = "hello, world!" """)
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEquals(TargetText("hello, world!"))))
        )
      )
    }

    "parse a predicate coerced to text with a field name" in {
      val query = TickleParser.parseQueryString("?fieldname = text(hello, world!)")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEquals(TargetText("hello, world!"))))
        )
      )
    }

    "parse an AND group" in {
      val query = TickleParser.parseQueryString("foo AND bar")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(AndGroup(List(
            Left(Expression(None, PredicateEquals(TargetText("foo")))),
            Left(Expression(None, PredicateEquals(TargetText("bar"))))
          )))
        )
      )
    }

    "parse an OR group" in {
      val query = TickleParser.parseQueryString("foo OR bar")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(OrGroup(List(
            Left(Expression(None, PredicateEquals(TargetText("foo")))),
            Left(Expression(None, PredicateEquals(TargetText("bar"))))
          )))
        )
      )
    }

    "parse a NOT group" in {
      val query = TickleParser.parseQueryString("NOT foo")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(NotGroup(
            Left(Expression(None, PredicateEquals(TargetText("foo"))))
          ))
        )
      )
    }

   "parse multiple NOT groups joined by AND" in {
      val query = TickleParser.parseQueryString("foo AND NOT bar AND NOT baz")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(AndGroup(List(
            Left(Expression(None, PredicateEquals(TargetText("foo")))),
            Right(NotGroup(
              Left(Expression(None, PredicateEquals(TargetText("bar"))))
            )),
            Right(NotGroup(
              Left(Expression(None, PredicateEquals(TargetText("baz"))))
            ))
          )))
        )
      )
    }

    "parse nested OR group with parentheses" in {
      val query = TickleParser.parseQueryString("foo AND (bar OR baz)")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(AndGroup(List(
            Left(Expression(None, PredicateEquals(TargetText("foo")))),
            Right(OrGroup(List(
              Left(Expression(None, PredicateEquals(TargetText("bar")))),
              Left(Expression(None, PredicateEquals(TargetText("baz"))))
            )))
          )))
        )
      )
    }

    "parse nested AND group with parentheses" in {
      val query = TickleParser.parseQueryString("(foo AND bar) OR baz")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(OrGroup(List(
            Right(AndGroup(List(
              Left(Expression(None, PredicateEquals(TargetText("foo")))),
              Left(Expression(None, PredicateEquals(TargetText("bar"))))
            ))),
            Left(Expression(None, PredicateEquals(TargetText("baz"))))
          )))
        )
      )
    }

    "parse trailing AND group without parentheses using operator precedence" in {
      val query = TickleParser.parseQueryString("foo OR bar AND baz")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(OrGroup(List(
            Left(Expression(None, PredicateEquals(TargetText("foo")))),
            Right(AndGroup(List(
              Left(Expression(None, PredicateEquals(TargetText("bar")))),
              Left(Expression(None, PredicateEquals(TargetText("baz")))))
            ))
          )))
        )
      )
    }

    "parse leading AND group without parentheses using operator precedence" in {
      val query = TickleParser.parseQueryString("foo AND bar OR baz")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Right(OrGroup(List(
            Right(AndGroup(List(
              Left(Expression(None, PredicateEquals(TargetText("foo")))),
              Left(Expression(None, PredicateEquals(TargetText("bar"))))
            ))),
            Left(Expression(None, PredicateEquals(TargetText("baz"))))
          )))
        )
      )
    }

    "parse a not-equals expression" in {
      val query = TickleParser.parseQueryString("?fieldname != 42")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateNotEquals(TargetInteger("42"))))
        )
      )
    }

    "parse a greater-than expression" in {
      val query = TickleParser.parseQueryString("?fieldname > 42")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateGreaterThan(TargetInteger("42"))))
        )
      )
    }

    "parse a less-than expression" in {
      val query = TickleParser.parseQueryString("?fieldname < 42")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateLessThan(TargetInteger("42"))))
        )
      )
    }

    "parse a greater-than-equals expression" in {
      val query = TickleParser.parseQueryString("?fieldname >= 42")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateGreaterThanEqualTo(TargetInteger("42"))))
        )
      )
    }

    "parse a less-than-equals expression" in {
      val query = TickleParser.parseQueryString("?fieldname <= 42")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateLessThanEqualTo(TargetInteger("42"))))
        )
      )
    }

    "parse an equals-range expression" in {
      val query = TickleParser.parseQueryString("?fieldname = [ 42 TO 44 ]")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEqualsRange(
            TargetRange(Some(TargetInteger("42")), Some(TargetInteger("44")), DataType.INTEGER, false, false))))
        )
      )
    }

    "parse an not-equals-range expression" in {
      val query = TickleParser.parseQueryString("?fieldname != [ bar TO foo ]")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateNotEqualsRange(
            TargetRange(Some(TargetText("bar")), Some(TargetText("foo")), DataType.TEXT, false, false))))
        )
      )
    }

    "parse a left-open range expression" in {
      val query = TickleParser.parseQueryString("?fieldname = [ TO foo ]")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEqualsRange(
            TargetRange(None, Some(TargetText("foo")), DataType.TEXT, false, false))))
        )
      )
    }

    "parse a right-open range expression" in {
      val query = TickleParser.parseQueryString("?fieldname = [ bar TO ]")
      logger.debug(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEqualsRange(
            TargetRange(Some(TargetText("bar")), None, DataType.TEXT, false, false))))
        )
      )
    }

  }

  "TickleParser.buildMatchers()" must {

   "parse a text value with a single term" in {
      val matchers = TickleParser.buildMatchers("?fieldname = foo", params)
      matchers must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("foo"))))
      )
    }

    "parse a text value with multiple terms" in {
      val matchers = TickleParser.buildMatchers(
        """?fieldname = "foo bar baz" """.stripMargin, params)
      inside(matchers) {
        case Some(PhraseMatcher(children)) =>
          children must have size(3)
          children must contain(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("foo"))).asInstanceOf[Matchers])
          children must contain(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("bar"))).asInstanceOf[Matchers])
          children must contain(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("baz"))).asInstanceOf[Matchers])
      }
    }

    "parse a phrase with a placeholder" in {
      val matchers = TickleParser.buildMatchers(
        """?fieldname = "foo bar _ baz" """.stripMargin, params)
      inside(matchers) {
        case Some(PhraseMatcher(children)) =>
          children must have size(4)
          children(0) must be(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("foo"))).asInstanceOf[Matchers])
          children(1) must be(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("bar"))).asInstanceOf[Matchers])
          children(2) must be(TermPlaceholder)
          children(3) must be(TermMatcher(FieldIdentifier("fieldname", DataType.TEXT), MatchTerm(text = Some("baz"))).asInstanceOf[Matchers])
      }
    }

    "parse a shorthand literal value" in {
      TickleParser.buildMatchers( """ ?fieldname = :foobar """.stripMargin, params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.LITERAL), MatchTerm(literal = Some("foobar"))))
      )
    }

    "parse a coerced literal value" in {
      TickleParser.buildMatchers( """ ?fieldname = literal(foo bar baz) """.stripMargin, params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.LITERAL), MatchTerm(literal = Some("foo bar baz"))))
      )
    }

    "parse a shorthand integer value" in {
      TickleParser.buildMatchers("?fieldname = 42", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.INTEGER), MatchTerm(integer = Some(42L))))
      )
    }

    "parse a coerced integer value" in {
      TickleParser.buildMatchers("?fieldname = integer(42)", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.INTEGER), MatchTerm(integer = Some(42L))))
      )
    }

    "parse a shorthand float value" in {
      TickleParser.buildMatchers( "?fieldname = 3.14159", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.FLOAT), MatchTerm(float = Some(3.14159))))
      )
    }

    "parse a coerced float value" in {
      TickleParser.buildMatchers( "?fieldname = float(3.14159)", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.FLOAT), MatchTerm(float = Some(3.14159))))
      )
    }

    "parse a shorthand datetime value" in {
      val date = new DateTime(1994, 11, 5, 8, 15, 30, DateTimeZone.UTC).toDate
      TickleParser.buildMatchers("?fieldname = 1994-11-05T08:15:30.0Z", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.DATETIME), MatchTerm(datetime = Some(date))))
      )
    }

    "parse a coerced datetime value" in {
      val date = new DateTime(1994, 11, 5, 8, 15, 30, DateTimeZone.UTC).toDate
      TickleParser.buildMatchers("?fieldname = datetime(1994-11-05T08:15:30.0Z)", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.DATETIME), MatchTerm(datetime = Some(date))))
      )
    }

    "parse a shorthand IPv4 address value" in {
      val address = DNSAddress.getByAddress("127.0.0.1")
      val matchers = TickleParser.buildMatchers("?fieldname = @127.0.0.1", params)
      inside(matchers) {
        case Some(TermMatcher(FieldIdentifier("fieldname", DataType.ADDRESS), MatchTerm(None, None, None, None, None, Some(bytes), None))) =>
          InetAddress.getByAddress(bytes) must be(address)
      }
    }

    "parse a coerced IPv4 address value" in {
      val address = DNSAddress.getByAddress("127.0.0.1")
      val matchers = TickleParser.buildMatchers("?fieldname = address(127.0.0.1)", params)
      inside(matchers) {
        case Some(TermMatcher(FieldIdentifier("fieldname", DataType.ADDRESS), MatchTerm(None, None, None, None, None, Some(bytes), None))) =>
          InetAddress.getByAddress(bytes) must be(address)
      }
    }

    "parse a shorthand IPv6 address value" in {
      val address = DNSAddress.getByAddress("::1")
      val matchers = TickleParser.buildMatchers("?fieldname = @::1", params)
      inside(matchers) {
        case Some(TermMatcher(FieldIdentifier("fieldname", DataType.ADDRESS), MatchTerm(None, None, None, None, None, Some(bytes), None))) =>
          InetAddress.getByAddress(bytes) must be(address)
      }
    }

    "parse a coerced IPv6 address value" in {
      val address = DNSAddress.getByAddress("::1")
      val matchers = TickleParser.buildMatchers("?fieldname = address(::1)", params)
      inside(matchers) {
        case Some(TermMatcher(FieldIdentifier("fieldname", DataType.ADDRESS), MatchTerm(None, None, None, None, None, Some(bytes), None))) =>
          InetAddress.getByAddress(bytes) must be(address)
      }
    }

    "parse a shorthand hostname value" in {
      TickleParser.buildMatchers("?fieldname = @www.google.com", params) must be(
        Some(PhraseMatcher(Seq(
          TermMatcher(FieldIdentifier("fieldname", DataType.HOSTNAME), MatchTerm(hostname = Some("com"))).asInstanceOf[Matchers],
          TermMatcher(FieldIdentifier("fieldname", DataType.HOSTNAME), MatchTerm(hostname = Some("google"))).asInstanceOf[Matchers],
          TermMatcher(FieldIdentifier("fieldname", DataType.HOSTNAME), MatchTerm(hostname = Some("www"))).asInstanceOf[Matchers]
        )))
      )
    }

    "parse a coerced hostname value" in {
      TickleParser.buildMatchers("?fieldname = hostname(www.google.com)", params) must be(
        Some(PhraseMatcher(Seq(
          TermMatcher(FieldIdentifier("fieldname", DataType.HOSTNAME), MatchTerm(hostname = Some("com"))).asInstanceOf[Matchers],
          TermMatcher(FieldIdentifier("fieldname", DataType.HOSTNAME), MatchTerm(hostname = Some("google"))).asInstanceOf[Matchers],
          TermMatcher(FieldIdentifier("fieldname", DataType.HOSTNAME), MatchTerm(hostname = Some("www"))).asInstanceOf[Matchers]
        )))
      )
    }
  }
}
