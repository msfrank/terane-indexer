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
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.Inside._
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import org.xbill.DNS.{Name, Address => DNSAddress}
import scala.Some
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.TickleParser._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.{AndMatcher, TermMatcher}

class TickleParserSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  // magic
  def this() = this(ActorSystem("TickleParserSpec"))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val params = TickleParserParams("fieldname")

  "TickleParser.parseQueryString()" must {

    "parse a bare text predicate" in {
      val query = TickleParser.parseQueryString("foobar")
      println(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(None, PredicateEquals(TargetText("foobar"))))
        )
      )
    }

    "parse a bare integer predicate" in {
      val query = TickleParser.parseQueryString("42")
      println(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(None, PredicateEquals(TargetInteger("42"))))
        )
      )
    }

    "parse a text predicate with a field name" in {
      val query = TickleParser.parseQueryString(":fieldname = foobar")
      println(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEquals(TargetText("foobar"))))
        )
      )
    }

    "parse a bare quoted text predicate" in {
      val query = TickleParser.parseQueryString(""" "hello, world!" """)
      println(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(None, PredicateEquals(TargetText("hello, world!"))))
        )
      )
    }

    "parse a quoted text subject with a field name" in {
      val query = TickleParser.parseQueryString(""" :fieldname = "hello, world!" """)
      println(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEquals(TargetText("hello, world!"))))
        )
      )
    }

    "parse a predicate coerced to text with a field name" in {
      val query = TickleParser.parseQueryString(":fieldname = text(hello, world!)")
      println(TickleParser.prettyPrint(query))
      query must be(
        Query(
          Left(Expression(Some("fieldname"), PredicateEquals(TargetText("hello, world!"))))
        )
      )
    }

    "parse an AND group" in {
      val query = TickleParser.parseQueryString("foo AND bar")
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
      println(TickleParser.prettyPrint(query))
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
  }

  "TickleParser.buildMatchers()" must {

   "parse a text value with a single term" in {
      val matchers = TickleParser.buildMatchers(":fieldname = foo", params)
      matchers must be(
        Some(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "foo"))
      )
    }

    "parse a text value with multiple terms" in {
      val matchers = TickleParser.buildMatchers(
        """:fieldname = "foo bar baz" """.stripMargin, params)
      inside(matchers) {
        case Some(AndMatcher(children)) =>
          children must have size(3)
          children must contain(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "foo").asInstanceOf[Matchers])
          children must contain(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "bar").asInstanceOf[Matchers])
          children must contain(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "baz").asInstanceOf[Matchers])
      }
    }

    "parse a literal value" in {
      TickleParser.buildMatchers( """ :fieldname = literal(foo bar baz) """.stripMargin, params) must be(
        Some(TermMatcher[String](FieldIdentifier("fieldname", DataType.LITERAL), "foo bar baz"))
      )
    }

    "parse an integer value" in {
      TickleParser.buildMatchers(":fieldname = 42", params) must be(
        Some(TermMatcher[Long](FieldIdentifier("fieldname", DataType.INTEGER), 42L))
      )
    }

    "parse a float value" in {
      TickleParser.buildMatchers( ":fieldname = 3.14159", params) must be(
        Some(TermMatcher[Double](FieldIdentifier("fieldname", DataType.FLOAT), 3.14159))
      )
    }

    "parse a datetime value" in {
      val date = new DateTime(1994, 11, 5, 8, 15, 30, DateTimeZone.UTC).toDate
      TickleParser.buildMatchers(":fieldname = 1994-11-05T08:15:30.0Z", params) must be(
        Some(TermMatcher(FieldIdentifier("fieldname", DataType.DATETIME), date))
      )
    }

    "parse a coerced IPv4 address value" in {
      val address = DNSAddress.getByAddress("127.0.0.1")
      val matchers = TickleParser.buildMatchers(":fieldname = address(127.0.0.1)", params)
      inside(matchers) {
        case Some(TermMatcher(FieldIdentifier("fieldname", DataType.ADDRESS), bytes: Array[Byte])) =>
          InetAddress.getByAddress(bytes) must be(address)
      }
    }

    "parse a hostname value" in {
      val hostname = Name.fromString("www.google.com")
      TickleParser.buildMatchers(":fieldname = @www.google.com", params) must be(
        Some(TermMatcher[Name](FieldIdentifier("fieldname", DataType.HOSTNAME), hostname))
      )
    }
  }
}
