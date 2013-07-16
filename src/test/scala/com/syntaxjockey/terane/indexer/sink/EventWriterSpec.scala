package com.syntaxjockey.terane.indexer.sink

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{Actor, Props, ActorSystem}
import akka.util.Timeout
import scala.concurrent.duration._
import com.netflix.astyanax.Keyspace
import org.joda.time.DateTime
import java.util.UUID

import com.syntaxjockey.terane.indexer.{UUIDLike, TestCluster}
import com.syntaxjockey.terane.indexer.sink.FieldManager.{GetFields, FieldsChanged}
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.bier.Event
import com.syntaxjockey.terane.indexer.sink.CassandraSink.{StoreEvent, WroteEvent}
import com.syntaxjockey.terane.indexer.cassandra.CassandraClient

class EventWriterSpec(_system: ActorSystem) extends TestKit(_system)
with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfter with BeforeAndAfterAll with TestCluster {

  // magic
  def this() = this(ActorSystem("EventWriterSpec"))

  var client: Option[CassandraClient] = None
  var keyspace: Option[Keyspace] = None
  var store: Option[Store] = None

  override def beforeAll() {
    client = Some(getCassandraClient)
  }

  before {
    val id = "test_" + new UUIDLike(UUID.randomUUID()).toString
    keyspace = Some(createKeyspace(client.get, id))
    store = Some(Store(id, id, DateTime.now()))
  }

//  after {
//    for (_keyspace <- keyspace)
//      _keyspace.dropKeyspace().getResult
//  }

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
    for (_client <- client)
      _client.close()
  }

  "An EventWriter" must {

    "write an event with a text field to the store" in {
      val writer = system.actorOf(Props(new Actor {
        val child = context.actorOf(Props(new EventWriter(store.get, keyspace.get, testActor)), "event-writer")
        def receive = {
          case x if this.sender == child => testActor forward x
          case x => child forward x
        }
      }), "testkit-proxy")
      expectMsg(GetFields)
      writer ! FieldsChanged(Map(textField.fieldId -> textField), Map(textField.text.get.id -> textField))
      createColumnFamily(keyspace.get, textField)
      val event = Event().set("text_field", "foo")
      writer ! StoreEvent(event, 1)
      expectMsgClass(30 seconds, classOf[WroteEvent]) must be(WroteEvent(event))
    }
  }
}
