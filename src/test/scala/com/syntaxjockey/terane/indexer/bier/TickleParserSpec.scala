package com.syntaxjockey.terane.indexer.bier

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import com.syntaxjockey.terane.indexer.bier.TickleParser._
import scala.Some
import com.syntaxjockey.terane.indexer.bier.TickleParser.AndGroup
import com.syntaxjockey.terane.indexer.bier.TickleParser.Query
import com.syntaxjockey.terane.indexer.bier.TickleParser.Subject
import com.syntaxjockey.terane.indexer.bier.TickleParser.OrGroup
import com.syntaxjockey.terane.indexer.bier.matchers.{AndMatcher, TermMatcher}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import org.joda.time.{DateTimeZone, DateTime}
import org.xbill.DNS.{Name, Address}
import java.net.InetAddress
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}

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
        Query(Left(Subject("foobar", Some("fieldname"), Some(EventValueType.TEXT)))))
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
          Left(Subject("hello, world!", Some("fieldname"), Some(EventValueType.TEXT)))
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
        Some(TermMatcher[String](FieldIdentifier("fieldname", EventValueType.TEXT), "foo"))
      )
    }

    "parse a text value with multiple terms" in {
      TickleParser.buildMatchers(
        """
          |fieldname[text]="foo bar baz"
        """.stripMargin) must be(
        Some(
          AndMatcher(List(
            TermMatcher[String](FieldIdentifier("fieldname", EventValueType.TEXT), "foo"),
            TermMatcher[String](FieldIdentifier("fieldname", EventValueType.TEXT), "bar"),
            TermMatcher[String](FieldIdentifier("fieldname", EventValueType.TEXT), "baz")
          ))
        )
      )
    }

    "parse a literal value" in {
      TickleParser.buildMatchers(
        """
          |fieldname[literal]="foo bar baz"
        """.stripMargin) must be(
        Some(TermMatcher[String](FieldIdentifier("fieldname", EventValueType.LITERAL), "foo bar baz"))
      )
    }

    "parse an integer value" in {
      TickleParser.buildMatchers("fieldname[integer]=42") must be(
        Some(TermMatcher[Long](FieldIdentifier("fieldname", EventValueType.INTEGER), 42L))
      )
    }

    "parse a quoted float value" in {
      TickleParser.buildMatchers(
        """
          |fieldname[float]="3.14159"
        """.stripMargin) must be(
        Some(TermMatcher[Double](FieldIdentifier("fieldname", EventValueType.FLOAT), 3.14159))
      )
    }

    "parse an unquoted float value" in {
      TickleParser.buildMatchers("fieldname[float]=3.14159") must be(
        Some(TermMatcher[Double](FieldIdentifier("fieldname", EventValueType.FLOAT), 3.14159))
      )
    }

    "parse a quoted datetime value" in {
      val datetime = new DateTime(1994, 11, 5, 8, 15, 30, DateTimeZone.UTC)
      TickleParser.buildMatchers(
        """
          |fieldname[datetime]="1994-11-05T08:15:30Z"
        """.stripMargin) must be(
        Some(TermMatcher[DateTime](FieldIdentifier("fieldname", EventValueType.DATETIME), datetime))
      )
    }

    "parse a quoted IPv4 address value" in {
      val address = Address.getByAddress("127.0.0.1")
      TickleParser.buildMatchers(
        """
          |fieldname[address]="127.0.0.1"
        """.stripMargin) must be(
        Some(TermMatcher[InetAddress](FieldIdentifier("fieldname", EventValueType.ADDRESS), address))
      )
    }

    "parse a hostname value" in {
      val hostname = Name.fromString("com")
      TickleParser.buildMatchers("fieldname[hostname]=com") must be(
        Some(TermMatcher[Name](FieldIdentifier("fieldname", EventValueType.HOSTNAME), hostname))
      )
    }
  }
}
