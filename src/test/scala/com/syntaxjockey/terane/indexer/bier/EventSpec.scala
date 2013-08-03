package com.syntaxjockey.terane.indexer.bier

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.joda.time.DateTime
import java.net.InetAddress
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Event._
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import org.xbill.DNS.Name

class EventSpec extends WordSpec with MustMatchers {

  "An Event" must {

    "set a text value" in {
      val event = Event(values = Map("text" -> "hello world"))
      event.values must contain key FieldIdentifier("text", EventValueType.TEXT)
      event.values(FieldIdentifier("text", EventValueType.TEXT)).text must be === Some("hello world")
    }

    "set a literal value" in {
      val event = Event(values = Map("literal" -> List("hello", "world")))
      event.values must contain key FieldIdentifier("literal", EventValueType.LITERAL)
      event.values(FieldIdentifier("literal", EventValueType.LITERAL)).literal must be === Some(List("hello", "world"))
    }

    "set an integer value" in {
      val event = Event(values = Map("integer" -> 42L))
      event.values must contain key FieldIdentifier("integer", EventValueType.INTEGER)
      event.values(FieldIdentifier("integer", EventValueType.INTEGER)).integer must be === Some(42.toLong)
    }

    "set a float value" in {
      val event = Event(values = Map("float" -> 3.14159))
      event.values must contain key FieldIdentifier("float", EventValueType.FLOAT)
      event.values(FieldIdentifier("float", EventValueType.FLOAT)).float must be === Some(3.14159)
    }

    "set a datetime value" in {
      val datetime = DateTime.now()
      val event = Event(values = Map("datetime" -> datetime))
      event.values must contain key FieldIdentifier("datetime", EventValueType.DATETIME)
      event.values(FieldIdentifier("datetime", EventValueType.DATETIME)).datetime must be === Some(datetime)
    }

    "set an address value" in {
      val address = InetAddress.getByName("127.0.0.1")
      val event = Event(values = Map("address" -> address))
      event.values must contain key FieldIdentifier("address", EventValueType.ADDRESS)
      event.values(FieldIdentifier("address", EventValueType.ADDRESS)).address must be === Some(address)
    }

    "set a hostname value" in {
      val hostname = Name.fromString("syntaxjockey.com")
      val event = Event(values = Map("hostname" -> hostname))
      event.values must contain key FieldIdentifier("hostname", EventValueType.HOSTNAME)
      event.values(FieldIdentifier("hostname", EventValueType.HOSTNAME)).hostname must be === Some(hostname)
    }
  }
}
