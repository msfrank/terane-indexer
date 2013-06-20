package com.syntaxjockey.terane.indexer.bier

import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpec
import com.syntaxjockey.terane.indexer.bier.TickleParser.{OrGroup, AndGroup, Subject}

class TickleParserSpec extends WordSpec with MustMatchers {

  "A TickleParser" must {

    "parse a bare subject" in {
      val query = TickleParser.parseQueryString("foobar")
      query.query must be(Left(Subject("foobar", None, None)))
    }

    "parse a qualified subject with a field name" in {
      val query = TickleParser.parseQueryString("fieldname=foobar")
      query.query must be(Left(Subject("foobar", Some("fieldname"), None)))
    }

    "parse a qualified subject with a field name and type" in {
      val query = TickleParser.parseQueryString("fieldname[text]=foobar")
      query.query must be(Left(Subject("foobar", Some("fieldname"), Some(EventValueType.TEXT))))
    }

    "parse an AND group" in {
      val query = TickleParser.parseQueryString("foo AND bar")
      query.query must be(Right(AndGroup(List(Left(Subject("foo",None,None)), Left(Subject("bar",None,None))))))
    }

    "parse an OR group" in {
      val query = TickleParser.parseQueryString("foo OR bar")
      query.query must be(Right(OrGroup(List(Left(Subject("foo",None,None)), Left(Subject("bar",None,None))))))
    }
  }
}
