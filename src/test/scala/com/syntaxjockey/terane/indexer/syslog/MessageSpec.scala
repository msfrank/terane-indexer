package com.syntaxjockey.terane.indexer.syslog

import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpec
import akka.util.ByteString
import akka.io.{PipelineFactory, PipelinePorts}
import org.joda.time.{DateTimeZone, DateTime}

/**
 * Created with IntelliJ IDEA.
 * User: msfrank
 * Date: 5/12/13
 * Time: 12:03 PM
 * To change this template use File | Settings | File Templates.
 */
class MessageSpec extends WordSpec with MustMatchers {

  def runSingleEvent(body: String): Message = {
    val stages = new ProcessBody()
    val PipelinePorts(_, evt, _) = PipelineFactory.buildFunctionTriple(new SyslogContext(), stages)
    val (messages: Iterable[Message], _) = evt(ByteString(body, "UTF-8"))
    val messageseq = messages.toSeq
    messageseq must have length(1)
    messageseq(0)
  }

  "The syslog ProcessBody pipeline" must {

    "parse a basic version 0 message" in {
      val message = runSingleEvent("<0> 2012-01-01T12:00:00Z localhost Hello, world!")
    }

    "parse a basic version 1 message" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00Z localhost - - - - Hello, world!")
      message.priority.facility must be(0)
      message.priority.severity must be(0)
      message.timestamp must be === new DateTime(2012, 1, 1, 12, 0, 0, 0, DateTimeZone.UTC)
      message.origin must be === "localhost"
      message.appName must be === Some("-")
      message.procId must be === Some("-")
      message.msgId must be === Some("-")
      message.elements must be === Map.empty
      message.message must be === Some("Hello, world!")
    }

    "parse a version 1 message with fractional seconds" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1Z localhost - - - - Hello, world!")
      message.timestamp.getMillisOfSecond must be === 100
    }

    "parse a version 1 message with non-UTC timezone" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - - Hello, world!")
    }

    "parse a version 1 message with a single structured data element and a message" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id foo=\"bar\"] Hello, world!")
      val id = SDIdentifier("id", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.params must be === Map("foo" -> "bar")
      message.message must be === Some("Hello, world!")
    }

    "parse a version 1 message with a single structured data element and no message" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id foo=\"bar\"]")
      val id = SDIdentifier("id", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.params must be === Map("foo" -> "bar")
      message.message must be === None
    }

    "parse a version 1 message with a structured data element which has a reserved identifier" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [reserved foo=\"bar\"]")
      val id = SDIdentifier("reserved", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.id.reserved must be(true)
    }

    "parse a version 1 message with a structured data element which has a non-reserved identifier" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id@31337 foo=\"bar\"]")
      val id = SDIdentifier("id", Some("31337"))
      message.elements must contain key(id)
      val element = message.elements(id)
      element.id.reserved must be(false)
    }

    "parse a version 1 message with a structured data element which has escaped characters in the param value" in {
      val message = runSingleEvent("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id foo=\"\\] \\\" \\\\ \"]")
      val id = SDIdentifier("id", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.params must be === Map("foo" -> "] \" \\ ")
    }
  }
}
