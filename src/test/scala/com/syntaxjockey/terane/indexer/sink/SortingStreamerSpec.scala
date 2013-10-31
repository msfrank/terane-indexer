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

package com.syntaxjockey.terane.indexer.sink

import akka.actor.{ActorRef, Actor, Props}
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.joda.time.{DateTimeZone, DateTime}
import scala.Some
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{BierEvent, Value, FieldIdentifier}
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.sink.Query._
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldMap
import com.syntaxjockey.terane.indexer.sink.CassandraSink.CreateQuery
import com.syntaxjockey.terane.indexer.TestCluster

class SortingStreamerSpec extends TestCluster("SortingStreamerSpec") with WordSpec with MustMatchers {

  "A SortingStreamer" must {

    val textId = FieldIdentifier("text", DataType.TEXT)
    val literalId = FieldIdentifier("literal", DataType.LITERAL)
    val integerId = FieldIdentifier("integer", DataType.INTEGER)
    val floatId = FieldIdentifier("float", DataType.FLOAT)
    val datetimeId = FieldIdentifier("datetime", DataType.DATETIME)
    val addressId = FieldIdentifier("address", DataType.ADDRESS)
    val hostnameId = FieldIdentifier("hostname", DataType.HOSTNAME)
    val fields = FieldMap(
      Map(
        textId -> CassandraField(textId, DateTime.now()),
        literalId -> CassandraField(literalId, DateTime.now()),
        integerId -> CassandraField(integerId, DateTime.now()),
        floatId -> CassandraField(floatId, DateTime.now()),
        datetimeId -> CassandraField(datetimeId, DateTime.now()),
        addressId -> CassandraField(addressId, DateTime.now()),
        hostnameId -> CassandraField(hostnameId, DateTime.now())
      ),
      Map.empty
    )

    def createSortingStreamer(createQuery: CreateQuery): ActorRef = {
      val id = UUID.randomUUID()
      val created = DateTime.now(DateTimeZone.UTC)
      system.actorOf(Props(new Actor {
        val child = context.actorOf(Props(new SortingStreamer(id, createQuery, created, fields)))
        def receive = {
          case x if this.sender == child => testActor forward x
          case x => child forward x
        }
      }))
    }

    "return events sorted by text field" in {
      val createQuery = CreateQuery("", "", None, Some(List(textId)), None, None)
      val sortingStreamer = createSortingStreamer(createQuery)
      val event1 = BierEvent(values = Map(textId -> Value(text = Some(Text("message 1")))))
      val event2 = BierEvent(values = Map(textId -> Value(text = Some(Text("message 2")))))
      val event3 = BierEvent(values = Map(textId -> Value(text = Some(Text("message 3")))))
      val event4 = BierEvent(values = Map(textId -> Value(text = Some(Text("message 4")))))
      val event5 = BierEvent(values = Map(textId -> Value(text = Some(Text("message 5")))))
      sortingStreamer ! event1
      expectMsg(NextEvent)
      sortingStreamer ! event4
      expectMsg(NextEvent)
      sortingStreamer ! event3
      expectMsg(NextEvent)
      sortingStreamer ! event5
      expectMsg(NextEvent)
      sortingStreamer ! event2
      expectMsg(NextEvent)
      sortingStreamer ! NoMoreEvents
      expectMsg(FinishedReading)
      sortingStreamer ! GetEvents(None)
      val batch = expectMsgClass(classOf[EventSet])
      batch.finished must be(true)
      batch.events must be(List(event1, event2, event3, event4, event5))
    }

    "return events sorted by integer field" in {
      val createQuery = CreateQuery("", "", None, Some(List(integerId)), None, None)
      val sortingStreamer = createSortingStreamer(createQuery)
      val event1 = BierEvent(values = Map(integerId -> Value(integer = Some(Integer(1)))))
      val event2 = BierEvent(values = Map(integerId -> Value(integer = Some(Integer(2)))))
      val event3 = BierEvent(values = Map(integerId -> Value(integer = Some(Integer(3)))))
      val event4 = BierEvent(values = Map(integerId -> Value(integer = Some(Integer(4)))))
      val event5 = BierEvent(values = Map(integerId -> Value(integer = Some(Integer(5)))))
      sortingStreamer ! event1
      expectMsg(NextEvent)
      sortingStreamer ! event4
      expectMsg(NextEvent)
      sortingStreamer ! event3
      expectMsg(NextEvent)
      sortingStreamer ! event5
      expectMsg(NextEvent)
      sortingStreamer ! event2
      expectMsg(NextEvent)
      sortingStreamer ! NoMoreEvents
      expectMsg(FinishedReading)
      sortingStreamer ! GetEvents(None)
      val batch = expectMsgClass(classOf[EventSet])
      batch.finished must be(true)
      batch.events must be(List(event1, event2, event3, event4, event5))
    }

    "return events sorted by float field" in {
      val createQuery = CreateQuery("", "", None, Some(List(floatId)), None, None)
      val sortingStreamer = createSortingStreamer(createQuery)
      val event1 = BierEvent(values = Map(floatId -> Value(float = Some(Float(0.1)))))
      val event2 = BierEvent(values = Map(floatId -> Value(float = Some(Float(0.2)))))
      val event3 = BierEvent(values = Map(floatId -> Value(float = Some(Float(0.3)))))
      val event4 = BierEvent(values = Map(floatId -> Value(float = Some(Float(0.4)))))
      val event5 = BierEvent(values = Map(floatId -> Value(float = Some(Float(0.5)))))
      sortingStreamer ! event1
      expectMsg(NextEvent)
      sortingStreamer ! event4
      expectMsg(NextEvent)
      sortingStreamer ! event3
      expectMsg(NextEvent)
      sortingStreamer ! event5
      expectMsg(NextEvent)
      sortingStreamer ! event2
      expectMsg(NextEvent)
      sortingStreamer ! NoMoreEvents
      expectMsg(FinishedReading)
      sortingStreamer ! GetEvents(None)
      val batch = expectMsgClass(classOf[EventSet])
      batch.finished must be(true)
      batch.events must be(List(event1, event2, event3, event4, event5))
    }

  }
}
