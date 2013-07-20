package com.syntaxjockey.terane.indexer.bier

import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpec
import com.syntaxjockey.terane.indexer.bier.TickleParser._
import scala.Some
import com.syntaxjockey.terane.indexer.bier.TickleParser.AndGroup
import com.syntaxjockey.terane.indexer.bier.TickleParser.Query
import com.syntaxjockey.terane.indexer.bier.TickleParser.Subject
import com.syntaxjockey.terane.indexer.bier.TickleParser.OrGroup

class TickleParserSpec extends WordSpec with MustMatchers {

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

    "parse nested groups with parentheses" in {
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

    "parse nested groups without parentheses using operator precedence" in {
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
  }
}
