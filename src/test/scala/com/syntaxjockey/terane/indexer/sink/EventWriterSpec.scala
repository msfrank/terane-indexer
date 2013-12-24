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

import scala.language.postfixOps

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import akka.actor.{ActorRef, Actor, Props}
import akka.agent.Agent
import com.netflix.astyanax.Keyspace
import org.joda.time.DateTime
import org.xbill.DNS.Name
import scala.concurrent.duration._
import java.util.UUID
import java.net.InetAddress

import com.syntaxjockey.terane.indexer.{RequiresTestCluster, UUIDLike, TestCluster}
import com.syntaxjockey.terane.indexer.bier.BierEvent
import com.syntaxjockey.terane.indexer.bier.BierEvent._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{StoreEvent, WroteEvent}
import com.syntaxjockey.terane.indexer.sink.FieldManager.{GetFields, FieldMap}
import com.syntaxjockey.terane.indexer.sink.StatsManager.{GetStats, StatsMap}

class EventWriterSpec extends TestCluster("EventWriterSpec") with WordSpec with MustMatchers with BeforeAndAfter with BeforeAndAfterAll {
  import scala.concurrent.ExecutionContext.Implicits.global

  /**
   * create a fixture with a new cassandra keyspace for testing
   *
   * @param runTest
   */
  def withWriter(runTest: (Keyspace,ActorRef) => Any) {
    val client = getCassandraClient
    val id = "test_" + new UUIDLike(UUID.randomUUID()).toString
    val keyspace = createKeyspace(client, id)
    val settings = CassandraSinkSettings(id, 5.seconds)
    val writer = system.actorOf(Props(new Actor {
      val child = context.actorOf(EventWriter.props(settings, keyspace, new SinkBus(), testActor, testActor), "writer_" + id)
      def receive = {
        case x if this.sender == child => testActor forward x
        case x => child forward x
      }
    }), "proxy_" + id)
    runTest(keyspace, writer)
    keyspace.dropKeyspace().getResult
  }

  "An EventWriter" must {

    "write an event with a text field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, textField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(textField.fieldId -> textField), Map(textField.text.get.id -> textField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(textCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("text_field" -> Text("foo")))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a literal field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, literalField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(literalField.fieldId -> literalField), Map(literalField.literal.get.id -> literalField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(literalCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("literal_field" -> Literal("foo")))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with an integer field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, integerField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(integerField.fieldId -> integerField), Map(integerField.integer.get.id -> integerField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(integerCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("integer_field" -> Integer(42)))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a float field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, floatField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(floatField.fieldId -> floatField), Map(floatField.float.get.id -> floatField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(floatCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("float_field" -> Float(3.14)))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a datetime field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, datetimeField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(datetimeField.fieldId -> datetimeField), Map(datetimeField.datetime.get.id -> datetimeField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(datetimeCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("datetime_field" -> Datetime(DateTime.now())))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with an address field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, addressField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(addressField.fieldId -> addressField), Map(addressField.address.get.id -> addressField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(addressCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("address_field" -> Address(InetAddress.getLocalHost)))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a hostname field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, hostnameField)
      expectMsg(GetFields)
      writer ! FieldMap(Map(hostnameField.fieldId -> hostnameField), Map(hostnameField.hostname.get.id -> hostnameField))
      expectMsg(GetStats)
      writer ! StatsMap(Map(hostnameCf.id -> Agent(FieldStatistics.empty)))
      val event = BierEvent(values = Map("hostname_field" -> Hostname(Name.fromString("syntaxjockey.com"))))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }
  }
}
