package com.syntaxjockey.terane.indexer

import scala.Some
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import com.syntaxjockey.terane.indexer.sink.FieldManager.{FieldsChanged, Field, TypedFieldColumnFamily}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier._

import com.syntaxjockey.terane.indexer.sink._
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.netflix.astyanax.{Cluster, Keyspace}
import com.syntaxjockey.terane.indexer.cassandra.{CassandraRowOperations, CassandraCFOperations, CassandraKeyspaceOperations, CassandraClient}
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperClient
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import scala.Some
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import scala.Some

trait TestCluster {
  import TestCluster._

  def getZookeeperClient = new ZookeeperClient(config.getConfig("terane.zookeeper"))

  def getCassandraClient = new CassandraClient(config.getConfig("terane.cassandra"))

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

  val literalId = FieldIdentifier("literal_field", EventValueType.LITERAL)
  val literalCf = new TypedFieldColumnFamily(literalId.fieldName, "literal_field", 0, new LiteralField(),
    new ColumnFamily[java.lang.Long,StringPosting]("literal_field", LongSerializer.get, FieldSerializers.Literal))
  val literalField = Field(literalId, DateTime.now(), literal = Some(literalCf))

  val integerId = FieldIdentifier("integer_field", EventValueType.INTEGER)
  val integerCf = new TypedFieldColumnFamily(integerId.fieldName, "integer_field", 0, new IntegerField(),
    new ColumnFamily[java.lang.Long,LongPosting]("integer_field", LongSerializer.get, FieldSerializers.Integer))
  val integerField = Field(integerId, DateTime.now(), integer = Some(integerCf))

  val floatId = FieldIdentifier("float_field", EventValueType.FLOAT)
  val floatCf = new TypedFieldColumnFamily(floatId.fieldName, "float_field", 0, new FloatField(),
    new ColumnFamily[java.lang.Long,DoublePosting]("float_field", LongSerializer.get, FieldSerializers.Float))
  val floatField = Field(floatId, DateTime.now(), float = Some(floatCf))

  val datetimeId = FieldIdentifier("datetime_field", EventValueType.DATETIME)
  val datetimeCf = new TypedFieldColumnFamily(datetimeId.fieldName, "datetime_field", 0, new DatetimeField(),
    new ColumnFamily[java.lang.Long,DatePosting]("datetime_field", LongSerializer.get, FieldSerializers.Datetime))
  val datetimeField = Field(datetimeId, DateTime.now(), datetime = Some(datetimeCf))

  val addressId = FieldIdentifier("address_field", EventValueType.ADDRESS)
  val addressCf = new TypedFieldColumnFamily(addressId.fieldName, "address_field", 0, new AddressField(),
    new ColumnFamily[java.lang.Long,AddressPosting]("address_field", LongSerializer.get, FieldSerializers.Address))
  val addressField = Field(addressId, DateTime.now(), address = Some(addressCf))

  val hostnameId = FieldIdentifier("hostname_field", EventValueType.HOSTNAME)
  val hostnameCf = new TypedFieldColumnFamily(hostnameId.fieldName, "hostname_field", 0, new HostnameField(),
    new ColumnFamily[java.lang.Long,StringPosting]("hostname_field", LongSerializer.get, FieldSerializers.Hostname))
  val hostnameField = Field(hostnameId, DateTime.now(), hostname = Some(hostnameCf))

  /**
   *
   * @param client
   * @param keyspaceName
   * @return
   */
  def createKeyspace(client: CassandraClient, keyspaceName: String): Keyspace = {
    client.createKeyspace(keyspaceName).getResult
    val _keyspace = new KeyspaceWithCFOperations(client.getKeyspace(keyspaceName))
    _keyspace.keyspace
  }

  /**
   *
   * @param keyspace
   * @param field
   */
  def createColumnFamily(keyspace: Keyspace, field: Field) {
    field.fieldId match {
      case FieldIdentifier(name, EventValueType.TEXT) =>
        keyspace.createTextField(name).getResult
      case FieldIdentifier(name, EventValueType.LITERAL) =>
        keyspace.createLiteralField(name).getResult
      case FieldIdentifier(name, EventValueType.INTEGER) =>
        keyspace.createIntegerField(name).getResult
      case FieldIdentifier(name, EventValueType.FLOAT) =>
        keyspace.createFloatField(name).getResult
      case FieldIdentifier(name, EventValueType.DATETIME) =>
        keyspace.createDatetimeField(name).getResult
      case FieldIdentifier(name, EventValueType.ADDRESS) =>
        keyspace.createAddressField(name).getResult
      case FieldIdentifier(name, EventValueType.HOSTNAME) =>
        keyspace.createHostnameField(name).getResult
    }
  }
}

object TestCluster {
  class ClusterWithKeyspaceOperations(val cluster: Cluster) extends CassandraKeyspaceOperations {}
  class KeyspaceWithCFOperations(val keyspace: Keyspace) extends CassandraCFOperations {}
  class KeyspaceWithRowOperations(val keyspace: Keyspace) extends CassandraRowOperations {}

  implicit def cluster2KeyspaceOperations(cluster: Cluster): ClusterWithKeyspaceOperations = new ClusterWithKeyspaceOperations(cluster)
  implicit def keyspace2CFOperations(keyspace: Keyspace): KeyspaceWithCFOperations = new KeyspaceWithCFOperations(keyspace)
  implicit def keyspace2RowOperations(keyspace: Keyspace): KeyspaceWithRowOperations = new KeyspaceWithRowOperations(keyspace)
}
