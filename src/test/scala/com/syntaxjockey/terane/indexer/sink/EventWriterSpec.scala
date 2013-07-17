package com.syntaxjockey.terane.indexer.sink

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import scala.concurrent.duration._
import com.netflix.astyanax.Keyspace
import org.joda.time.DateTime
import java.util.UUID

import com.syntaxjockey.terane.indexer.{UUIDLike, TestCluster}
import com.syntaxjockey.terane.indexer.sink.FieldManager.{GetFields, FieldsChanged}
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.bier.Event
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{StoreEvent, WroteEvent}
import java.net.InetAddress
import org.xbill.DNS.Name

class EventWriterSpec(_system: ActorSystem) extends TestKit(_system)
with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfter with BeforeAndAfterAll with TestCluster {

  // magic
  def this() = this(ActorSystem("EventWriterSpec"))

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

    "write an event with a text field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, textField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(textField.fieldId -> textField), Map(textField.text.get.id -> textField))
      val event = Event().set("text_field", "foo")
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a literal field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, literalField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(literalField.fieldId -> literalField), Map(literalField.literal.get.id -> literalField))
      val event = Event().set("literal_field", List("foo", "bar", "baz"))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with an integer field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, integerField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(integerField.fieldId -> integerField), Map(integerField.integer.get.id -> integerField))
      val event = Event().set("integer_field", 42 toLong)
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a float field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, floatField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(floatField.fieldId -> floatField), Map(floatField.float.get.id -> floatField))
      val event = Event().set("float_field", 3.14)
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a datetime field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, datetimeField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(datetimeField.fieldId -> datetimeField), Map(datetimeField.datetime.get.id -> datetimeField))
      val event = Event().set("datetime_field", DateTime.now())
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with an address field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, addressField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(addressField.fieldId -> addressField), Map(addressField.address.get.id -> addressField))
      val event = Event().set("address_field", InetAddress.getLocalHost)
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }

    "write an event with a hostname field to the store" in withWriter { (keyspace, writer) =>
      createColumnFamily(keyspace, hostnameField)
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(hostnameField.fieldId -> hostnameField), Map(hostnameField.hostname.get.id -> hostnameField))
      val event = Event().set("hostname_field", Name.fromString("syntaxjockey.com"))
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }
  }
}
