/*
 * Copyright (c) 2010-2014 Michael Frank <msfrank@syntaxjockey.com>
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
 *
 */

package com.syntaxjockey.terane.indexer.route

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer.zookeeper.ZNode
import com.syntaxjockey.terane.indexer.source.SourceSettings
import com.syntaxjockey.terane.indexer.sink.SinkSettings
import com.syntaxjockey.terane.indexer._

class RouteSpec extends TestCluster("RouteSpec") with WordSpec with MustMatchers {

  val emptyZNode = ZNode(0, 0, 0, 0, 0, 0, 0, 0 , 0 , 0, 0)
  object TestSourceSettings extends SourceSettings { val name = "testSource" }
  object TestSinkSettings extends SinkSettings { val name = "testSink" }

  case class TestEvent(source: String, event: BierEvent) extends SourceEvent

  "A Route matching all" must {

    "store an event to all sinks" in {
      val route = RouteContext(Vector(MatchesAll), StoreAllAction)
      val source = SourceRef(testActor, Source(emptyZNode, TestSourceSettings))
      val event = BierEvent(None)
      val sinks = SinkMap(Map("sink" -> SinkRef(testActor, Sink(UUID.randomUUID(), emptyZNode, TestSinkSettings))))
      route.process(source, TestEvent("source", event), sinks)
      expectMsg(event)
    }

    "store an event to the specified sink" in {
      val route = RouteContext(Vector(MatchesAll), StoreAction(Vector("sink")))
      val source = SourceRef(testActor, Source(emptyZNode, TestSourceSettings))
      val event = BierEvent(None)
      val sinks = SinkMap(Map("sink" -> SinkRef(testActor, Sink(UUID.randomUUID(), emptyZNode, TestSinkSettings))))
      route.process(source, TestEvent("source", event), sinks)
      expectMsg(event)
    }

    "drop an event" in {
      val route = RouteContext(Vector(MatchesAll), DropAction)
      val source = SourceRef(testActor, Source(emptyZNode, TestSourceSettings))
      val event = BierEvent(None)
      val sinks = SinkMap(Map("sink" -> SinkRef(testActor, Sink(UUID.randomUUID(), emptyZNode, TestSinkSettings))))
      route.process(source, TestEvent("source", event), sinks)
      expectNoMsg()
    }

    "not store an event if the sink is not defined" in {
      val route = RouteContext(Vector(MatchesAll), StoreAction(Vector("sink1")))
      val source = SourceRef(testActor, Source(emptyZNode, TestSourceSettings))
      val event = BierEvent(None)
      val sinks = SinkMap(Map("sink2" -> SinkRef(testActor, Sink(UUID.randomUUID(), emptyZNode, TestSinkSettings))))
      route.process(source, TestEvent("source", event), sinks)
      expectNoMsg()
    }

  }
}
