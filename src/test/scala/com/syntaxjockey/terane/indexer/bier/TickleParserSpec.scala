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
import org.xbill.DNS.{Name, Address}
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

  "A TickleParser" must {

    "parse a bare subject" in {
      TickleParser.parseQueryString("foobar") must be(
        Query(Left(Subject("foobar", None, None))))
    }

    "parse a qualified subject with a field name" in {
      TickleParser.parseQueryString("fieldname=foobar") must be(
        Query(Left(Subject("foobar", Some("fieldname"), None))))
    }

    "parse a qualified subject with a field name and type" in {
      TickleParser.parseQueryString("fieldname[text]=foobar") must be(
        Query(Left(Subject("foobar", Some("fieldname"), Some(DataType.TEXT)))))
    }

    "parse a bare quoted subject" in {
      TickleParser.parseQueryString(
        """
          |"hello, world!"
        """.stripMargin) must be(
        Query(
          Left(Subject("hello, world!", None, None))
        )
      )
    }

    "parse a quoted subject with a field name" in {
      TickleParser.parseQueryString(
        """
          |fieldname="hello, world!"
        """.stripMargin) must be(
        Query(
          Left(Subject("hello, world!", Some("fieldname"), None))
        )
      )
    }

    "parse a quoted subject with a field name and type" in {
      TickleParser.parseQueryString(
        """
          |fieldname[text]="hello, world!"
        """.stripMargin) must be(
        Query(
          Left(Subject("hello, world!", Some("fieldname"), Some(DataType.TEXT)))
        )
      )
    }

    "parse an AND group" in {
      TickleParser.parseQueryString("foo AND bar") must be(
        Query(
          Right(AndGroup(List(
            Left(Subject("foo",None,None)),
            Left(Subject("bar",None,None))
          )))
        )
      )
    }

    "parse an OR group" in {
      TickleParser.parseQueryString("foo OR bar") must be(
        Query(
          Right(OrGroup(List(
            Left(Subject("foo",None,None)),
            Left(Subject("bar",None,None))
          )))
        )
      )
    }

    "parse a NOT group" in {
      TickleParser.parseQueryString("NOT foo") must be(
        Query(
          Right(NotGroup(List(
            Left(Subject("foo",None,None))
          )))
        )
      )
    }

    "parse nested OR group with parentheses" in {
      TickleParser.parseQueryString("foo AND (bar OR baz)") must be(
        Query(
          Right(AndGroup(List(
            Left(Subject("foo", None, None)),
            Right(OrGroup(List(
              Left(Subject("bar",None,None)),
              Left(Subject("baz",None,None))
            )))
          )))
        )
      )
    }

    "parse nested AND group with parentheses" in {
      TickleParser.parseQueryString("(foo AND bar) OR baz") must be(
        Query(
          Right(OrGroup(List(
            Right(AndGroup(List(
              Left(Subject("foo",None,None)),
              Left(Subject("bar",None,None))
            ))),
            Left(Subject("baz", None, None))
          )))
        )
      )
    }

    "parse trailing AND group without parentheses using operator precedence" in {
      TickleParser.parseQueryString("foo OR bar AND baz") must be(
        Query(
          Right(OrGroup(List(
            Left(Subject("foo", None, None)),
            Right(AndGroup(List(
              Left(Subject("bar",None,None)),
              Left(Subject("baz",None,None))))
            )
          )))
        )
      )
    }

    "parse leading AND group without parentheses using operator precedence" in {
      TickleParser.parseQueryString("foo AND bar OR baz") must be(
        Query(
          Right(OrGroup(List(
            Right(AndGroup(List(
              Left(Subject("foo",None,None)),
              Left(Subject("bar",None,None))
            ))),
            Left(Subject("baz", None, None))
          )))
        )
      )
    }

    "parse a text value with a single term" in {
      TickleParser.buildMatchers("fieldname[text]=foo") must be(
        Some(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "foo"))
      )
    }

    "parse a text value with multiple terms" in {
      val matchers = TickleParser.buildMatchers(
        """
          |fieldname[text]="foo bar baz"
        """.stripMargin)
      inside(matchers) {
        case Some(AndMatcher(children)) =>
          children must have length(3)
          children must contain(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "foo").asInstanceOf[Matchers])
          children must contain(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "bar").asInstanceOf[Matchers])
          children must contain(TermMatcher[String](FieldIdentifier("fieldname", DataType.TEXT), "baz").asInstanceOf[Matchers])
      }
    }

    "parse a literal value" in {
      TickleParser.buildMatchers(
        """
          |fieldname[literal]="foo bar baz"
        """.stripMargin) must be(
        Some(TermMatcher[String](FieldIdentifier("fieldname", DataType.LITERAL), "foo bar baz"))
      )
    }

    "parse an integer value" in {
      TickleParser.buildMatchers("fieldname[integer]=42") must be(
        Some(TermMatcher[Long](FieldIdentifier("fieldname", DataType.INTEGER), 42L))
      )
    }

    "parse a quoted float value" in {
      TickleParser.buildMatchers(
        """
          |fieldname[float]="3.14159"
        """.stripMargin) must be(
        Some(TermMatcher[Double](FieldIdentifier("fieldname", DataType.FLOAT), 3.14159))
      )
    }

    "parse an unquoted float value" in {
      TickleParser.buildMatchers("fieldname[float]=3.14159") must be(
        Some(TermMatcher[Double](FieldIdentifier("fieldname", DataType.FLOAT), 3.14159))
      )
    }

    "parse a quoted datetime value" in {
      val datetime = new DateTime(1994, 11, 5, 8, 15, 30, DateTimeZone.UTC)
      val matchers = TickleParser.buildMatchers(
        """
          |fieldname[datetime]="1994-11-05T08:15:30Z"
        """.stripMargin)
      inside(matchers) {
        case Some(TermMatcher(FieldIdentifier("fieldname", DataType.DATETIME), _datetime: DateTime)) =>
          datetime must equal(_datetime)
      }
    }

    "parse a quoted IPv4 address value" in {
      val address = Address.getByAddress("127.0.0.1")
      TickleParser.buildMatchers(
        """
          |fieldname[address]="127.0.0.1"
        """.stripMargin) must be(
        Some(TermMatcher[InetAddress](FieldIdentifier("fieldname", DataType.ADDRESS), address))
      )
    }

    "parse a hostname value" in {
      val hostname = Name.fromString("com")
      TickleParser.buildMatchers("fieldname[hostname]=com") must be(
        Some(TermMatcher[Name](FieldIdentifier("fieldname", DataType.HOSTNAME), hostname))
      )
    }
  }
}
