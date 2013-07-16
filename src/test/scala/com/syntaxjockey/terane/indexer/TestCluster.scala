package com.syntaxjockey.terane.indexer

import scala.Some
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import com.syntaxjockey.terane.indexer.sink.FieldManager.{FieldsChanged, Field, TypedFieldColumnFamily}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.{EventValueType, TextField}

import com.syntaxjockey.terane.indexer.sink.{FieldSerializers, StringPosting}
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.netflix.astyanax.{Cluster, Keyspace}
import com.syntaxjockey.terane.indexer.cassandra.{CassandraCFOperations, CassandraKeyspaceOperations, CassandraClient}
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperClient

trait TestCluster {
  import TestCluster._

  val config = ConfigFactory.parseString(
    """
      |terane {
      |  cassandra {
      |    connection-pool-name = "Default Connection Pool"
      |    port = 9160
      |    max-conns-per-host = 1
      |    seeds = [ "127.0.0.1:9160" ]
      |    cluster-name = "Default Cluster"
      |  }
      |}
    """.stripMargin)

  val textId = FieldIdentifier("text_field", EventValueType.TEXT)
  val textCf = new TypedFieldColumnFamily(textId.fieldName, "text_field", 0, new TextField(),
    new ColumnFamily[java.lang.Long,StringPosting]("text_field", LongSerializer.get, FieldSerializers.Text))
  val textField = Field(textId, DateTime.now(), text = Some(textCf))

  /*
  literal: Option[TypedFieldColumnFamily[LiteralField,StringPosting]] = None,
  integer: Option[TypedFieldColumnFamily[IntegerField,LongPosting]] = None,
  float: Option[TypedFieldColumnFamily[FloatField,DoublePosting]] = None,
  datetime: Option[TypedFieldColumnFamily[DatetimeField,DatePosting]] = None,
  address: Option[TypedFieldColumnFamily[AddressField,AddressPosting]] = None,
  hostname: Option[TypedFieldColumnFamily[HostnameField,StringPosting]] = None)
  */

  def getZookeeperClient = new ZookeeperClient(config.getConfig("terane.zookeeper"))

  def getCassandraClient = new CassandraClient(config.getConfig("terane.cassandra"))

  def createKeyspace(client: CassandraClient, keyspaceName: String): Keyspace = {
    client.createKeyspace(keyspaceName).getResult
    val _keyspace = new _Keyspace(client.getKeyspace(keyspaceName))
    _keyspace.keyspace
  }

  def createColumnFamily(keyspace: Keyspace, field: Field) {
    val _keyspace = new _Keyspace(keyspace)
    field.fieldId match {
      case FieldIdentifier(name, EventValueType.TEXT) =>
        _keyspace.createTextField(name).getResult
      case FieldIdentifier(name, EventValueType.LITERAL) =>
        _keyspace.createLiteralField(name).getResult
      case FieldIdentifier(name, EventValueType.INTEGER) =>
        _keyspace.createIntegerField(name).getResult
      case FieldIdentifier(name, EventValueType.FLOAT) =>
        _keyspace.createFloatField(name).getResult
      case FieldIdentifier(name, EventValueType.DATETIME) =>
        _keyspace.createDatetimeField(name).getResult
      case FieldIdentifier(name, EventValueType.ADDRESS) =>
        _keyspace.createAddressField(name).getResult
      case FieldIdentifier(name, EventValueType.HOSTNAME) =>
        _keyspace.createHostnameField(name).getResult
    }
  }
}

object TestCluster {
  private class _Keyspace(val keyspace: Keyspace) extends CassandraCFOperations {}
}
