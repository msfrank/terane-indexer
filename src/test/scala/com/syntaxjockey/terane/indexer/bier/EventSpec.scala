package com.syntaxjockey.terane.indexer.bier

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.joda.time.DateTime
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.bier.Event._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import org.xbill.DNS.Name

class EventSpec extends WordSpec with MustMatchers {

  "An Event" must {

    "set a text value" in {
      val event = Event(values = Map("text" -> Text("hello world")))
      event.values must contain key FieldIdentifier("text", DataType.TEXT)
      event.values(FieldIdentifier("text", DataType.TEXT)).text must be === Some(Text("hello world"))
    }

    "set a literal value" in {
      val event = Event(values = Map("literal" -> Literal("hello world")))
      event.values must contain key FieldIdentifier("literal", DataType.LITERAL)
      event.values(FieldIdentifier("literal", DataType.LITERAL)).literal must be === Some(Literal("hello world"))
    }

    "set an integer value" in {
      val event = Event(values = Map("integer" -> Integer(42)))
      event.values must contain key FieldIdentifier("integer", DataType.INTEGER)
      event.values(FieldIdentifier("integer", DataType.INTEGER)).integer must be === Some(Integer(42))
    }

    "set a float value" in {
      val event = Event(values = Map("float" -> Float(3.14159)))
      event.values must contain key FieldIdentifier("float", DataType.FLOAT)
      event.values(FieldIdentifier("float", DataType.FLOAT)).float must be === Some(Float(3.14159))
    }

    "set a datetime value" in {
      val datetime = DateTime.now()
      val event = Event(values = Map("datetime" -> Datetime(datetime)))
      event.values must contain key FieldIdentifier("datetime", DataType.DATETIME)
      event.values(FieldIdentifier("datetime", DataType.DATETIME)).datetime must be === Some(Datetime(datetime))
    }

    "set an address value" in {
      val address = InetAddress.getByName("127.0.0.1")
      val event = Event(values = Map("address" -> Address(address)))
      event.values must contain key FieldIdentifier("address", DataType.ADDRESS)
      event.values(FieldIdentifier("address", DataType.ADDRESS)).address must be === Some(Address(address))
    }

    "set a hostname value" in {
      val hostname = Name.fromString("syntaxjockey.com")
      val event = Event(values = Map("hostname" -> Hostname(hostname)))
      event.values must contain key FieldIdentifier("hostname", DataType.HOSTNAME)
      event.values(FieldIdentifier("hostname", DataType.HOSTNAME)).hostname must be === Some(Hostname(hostname))
    }
  }
}
