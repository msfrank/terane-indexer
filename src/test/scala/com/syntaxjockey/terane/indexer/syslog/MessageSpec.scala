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

package com.syntaxjockey.terane.indexer.syslog

import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpec
import akka.testkit.TestActorRef
import akka.actor.{Actor, ActorLogging}
import akka.io.{PipelineFactory, PipelinePorts}
import akka.util.ByteString
import org.joda.time.{DateTimeZone, DateTime}

import com.syntaxjockey.terane.indexer.TestCluster

class MessageSpec extends TestCluster("MessageSpec") with WordSpec with MustMatchers {

  def runFrame(body: String): Message = {
    val actor = TestActorRef[Blackhole]
    val log = actor.underlyingActor.log
    val ctx = actor.underlyingActor.context
    val stages = new ProcessFrames()
    val PipelinePorts(_, evt, _) = PipelineFactory.buildFunctionTriple(new SyslogContext(log, ctx), stages)
    val (_messages: Iterable[SyslogMessages], _) = evt(SyslogFrames(Seq(ByteString(body, "UTF-8")), ByteString.empty))
    val messages = _messages.toSeq
    messages must have length 1
    messages(0).messages must have length 1
    messages(0).leftover must be(ByteString.empty)
    messages(0).messages.head
  }

  def runTcp(body: String): SyslogMessages = {
    val actor = TestActorRef[Blackhole]
    val log = actor.underlyingActor.log
    val ctx = actor.underlyingActor.context
    val stages = new ProcessFrames() >> new ProcessTcp()
    val PipelinePorts(_, evt, _) = PipelineFactory.buildFunctionTriple(new SyslogContext(log, ctx), stages)
    val (_messages: Iterable[SyslogMessages], _) = evt(ByteString(body, "UTF-8"))
    val messages = _messages.toSeq
    messages must have length 1
    messages(0)
  }
  
  def runUdp(body: String): SyslogMessages = {
    val actor = TestActorRef[Blackhole]
    val log = actor.underlyingActor.log
    val ctx = actor.underlyingActor.context
    val stages = new ProcessFrames() >> new ProcessUdp()
    val PipelinePorts(_, evt, _) = PipelineFactory.buildFunctionTriple(new SyslogContext(log, ctx), stages)
    val (_messages: Iterable[SyslogMessages], _) = evt(ByteString(body, "UTF-8"))
    val messages = _messages.toSeq
    messages must have length 1
    messages(0)
  }
  
  "The ProcessUdp pipeline" must {

    "parse a message" in {
      val messages = runUdp("<0> 2012-01-01T12:00:00Z localhost Hello, world!")
      messages.messages must have length 1
      messages.leftover must be(ByteString.empty)
    }
  }
  
  "The ProcessTcp pipeline" must {
    
    "parse a single length-prefixed message" in {
      val messages = runTcp("48 <0> 2012-01-01T12:00:00Z localhost Hello, world!")
      messages.messages must have length 1
      messages.leftover must be(ByteString.empty)
    }

    "parse multiple length-prefixed messages" in {
      val messages = runTcp("48 <0> 2012-01-01T12:00:00Z localhost Hello, world!65 <0> 2012-01-01T12:00:00Z localhost Hello, world this is a test...")
      messages.messages must have length 2
      messages.leftover must be(ByteString.empty)
    }
  }

  "The ProcessFrames pipeline" must {

    "parse a basic version 0 message" in {
      val message = runFrame("<0> 2012-01-01T12:00:00Z localhost Hello, world!")
    }

    "parse a basic version 1 message" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00Z localhost - - - - Hello, world!")
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
      val message = runFrame("<0>1 2012-01-01T12:00:00.1Z localhost - - - - Hello, world!")
      message.timestamp.getMillisOfSecond must be === 100
    }

    "parse a version 1 message with non-UTC timezone" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - - Hello, world!")
    }

    "parse a version 1 message with a single structured data element and a message" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id foo=\"bar\"] Hello, world!")
      val id = SDIdentifier("id", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.params must be === Map("foo" -> "bar")
      message.message must be === Some("Hello, world!")
    }

    "parse a version 1 message with a single structured data element and no message" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id foo=\"bar\"]")
      val id = SDIdentifier("id", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.params must be === Map("foo" -> "bar")
      message.message must be === None
    }

    "parse a version 1 message with a structured data element which has a reserved identifier" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [reserved foo=\"bar\"]")
      val id = SDIdentifier("reserved", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.id.reserved must be(true)
    }

    "parse a version 1 message with a structured data element which has a non-reserved identifier" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id@31337 foo=\"bar\"]")
      val id = SDIdentifier("id", Some("31337"))
      message.elements must contain key(id)
      val element = message.elements(id)
      element.id.reserved must be(false)
    }

    "parse a version 1 message with a structured data element which has escaped characters in the param value" in {
      val message = runFrame("<0>1 2012-01-01T12:00:00.1-08:00 localhost - - - [id foo=\"\\] \\\" \\\\ \"]")
      val id = SDIdentifier("id", None)
      message.elements must contain key(id)
      val element = message.elements(id)
      element.params must be === Map("foo" -> "] \" \\ ")
    }
  }
}

class Blackhole extends Actor with ActorLogging {
  def receive = { case _ => }
}
