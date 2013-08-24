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

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import com.netflix.astyanax.Keyspace
import org.joda.time.DateTime
import org.xbill.DNS.Name
import scala.concurrent.duration._
import java.net.InetAddress
import java.util.UUID

import com.syntaxjockey.terane.indexer.{RequiresTestCluster, UUIDLike, TestCluster}
import com.syntaxjockey.terane.indexer.bier.Event
import com.syntaxjockey.terane.indexer.bier.Event._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{StoreEvent, WroteEvent}
import com.syntaxjockey.terane.indexer.sink.FieldManager.{GetFields, FieldsChanged}
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store

class EventWriterSpec extends TestCluster("EventWriterSpec") with WordSpec with MustMatchers with BeforeAndAfter with BeforeAndAfterAll {

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  /**
   * create a fixture with a new cassandra keyspace for testing
   *
   * @param runTest
   */
  def withWriter(runTest: (Keyspace,ActorRef) => Any) {
    val client = getCassandraClient
    val id = "test_" + new UUIDLike(UUID.randomUUID()).toString
    val keyspace = createKeyspace(client, id)
    val store = Store(id, id, DateTime.now())
    val writer = system.actorOf(Props(new Actor {
      val child = context.actorOf(Props(new EventWriter(store, keyspace, testActor)), "writer_" + id)
      def receive = {
        case x if this.sender == child => testActor forward x
        case x => child forward x
      }
    }), "proxy_" + id)
    runTest(keyspace, writer)
    keyspace.dropKeyspace().getResult
    client.close()
  }

  "An EventWriter" must {

    "write an event with a text field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, textField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(textField.fieldId -> textField), Map(textField.text.get.id -> textField))
      val event = Event(values = Map("text_field" -> Text("foo")))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a literal field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, literalField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(literalField.fieldId -> literalField), Map(literalField.literal.get.id -> literalField))
      val event = Event(values = Map("literal_field" -> Literal("foo")))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with an integer field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, integerField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(integerField.fieldId -> integerField), Map(integerField.integer.get.id -> integerField))
      val event = Event(values = Map("integer_field" -> Integer(42)))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a float field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, floatField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(floatField.fieldId -> floatField), Map(floatField.float.get.id -> floatField))
      val event = Event(values = Map("float_field" -> Float(3.14)))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a datetime field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, datetimeField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(datetimeField.fieldId -> datetimeField), Map(datetimeField.datetime.get.id -> datetimeField))
      val event = Event(values = Map("datetime_field" -> Datetime(DateTime.now())))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with an address field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, addressField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(addressField.fieldId -> addressField), Map(addressField.address.get.id -> addressField))
      val event = Event(values = Map("address_field" -> Address(InetAddress.getLocalHost)))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a hostname field to the store" taggedAs RequiresTestCluster in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, hostnameField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(hostnameField.fieldId -> hostnameField), Map(hostnameField.hostname.get.id -> hostnameField))
      val event = Event(values = Map("hostname_field" -> Hostname(Name.fromString("syntaxjockey.com"))))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }
  }
}
