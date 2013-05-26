package com.syntaxjockey.terane.indexer.bier

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import java.util.UUID
import org.joda.time.DateTime
import java.net.{InetAddress, Inet4Address}

/**
 * Created with IntelliJ IDEA.
 * User: msfrank
 * Date: 5/25/13
 * Time: 4:40 PM
 * To change this template use File | Settings | File Templates.
 */
class EventSpec extends WordSpec with MustMatchers {

  "An Event" must {

    "set a text value" in {
      val event = new Event(UUID.randomUUID())
      event.set("text", "hello world")
      event must contain key ("text")
      val value: Option[Event.Value] = event.get("text")
      value.get.text must be === Some("hello world")
    }

    "set a literal value" in {
      val event = new Event(UUID.randomUUID())
      event.set("literal", Set("hello", "world"))
      event must contain key ("literal")
      val value: Option[Event.Value] = event.get("literal")
      value.get.literal must be === Some(Set("hello", "world"))
    }

    "set an integer value" in {
      val event = new Event(UUID.randomUUID())
      event.set("integer", 42.toLong)
      event must contain key ("integer")
      val value: Option[Event.Value] = event.get("integer")
      value.get.integer must be === Some(42.toLong)
    }

    "set a float value" in {
      val event = new Event(UUID.randomUUID())
      event.set("float", 3.14159)
      event must contain key ("float")
      val value: Option[Event.Value] = event.get("float")
      value.get.float must be === Some(3.14159)
    }

    "set a datetime value" in {
      val event = new Event(UUID.randomUUID())
      val datetime = DateTime.now()
      event.set("datetime", datetime)
      event must contain key ("datetime")
      val value: Option[Event.Value] = event.get("datetime")
      value.get.datetime must be === Some(datetime)
    }

    "set an address value" in {
      val event = new Event(UUID.randomUUID())
      event.set("address", InetAddress.getByName("127.0.0.1"))
      event must contain key ("address")
      val value: Option[Event.Value] = event.get("address")
      value.get.address must be === Some(InetAddress.getByName("127.0.0.1"))
    }
  }
}
