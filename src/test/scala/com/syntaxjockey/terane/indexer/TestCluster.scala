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

package com.syntaxjockey.terane.indexer

import com.typesafe.config.ConfigFactory
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import com.netflix.astyanax.{Cluster, Keyspace}
import org.joda.time.DateTime
import scala.Some

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.sink._
import com.syntaxjockey.terane.indexer.sink.FieldManager.TypedFieldColumnFamily
import com.syntaxjockey.terane.indexer.cassandra.{CassandraRowOperations, CassandraCFOperations, CassandraKeyspaceOperations, CassandraClient}
import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.sink.FieldManager.Field
import org.scalatest.Tag

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

  val textId = FieldIdentifier("text_field", DataType.TEXT)
  val textCf = new TypedFieldColumnFamily(textId.fieldName, "text_field", 0, new TextField(),
    new ColumnFamily[java.lang.Long,StringPosting]("text_field", LongSerializer.get, FieldSerializers.Text))
  val textField = Field(textId, DateTime.now(), text = Some(textCf))

  val literalId = FieldIdentifier("literal_field", DataType.LITERAL)
  val literalCf = new TypedFieldColumnFamily(literalId.fieldName, "literal_field", 0, new LiteralField(),
    new ColumnFamily[java.lang.Long,StringPosting]("literal_field", LongSerializer.get, FieldSerializers.Literal))
  val literalField = Field(literalId, DateTime.now(), literal = Some(literalCf))

  val integerId = FieldIdentifier("integer_field", DataType.INTEGER)
  val integerCf = new TypedFieldColumnFamily(integerId.fieldName, "integer_field", 0, new IntegerField(),
    new ColumnFamily[java.lang.Long,LongPosting]("integer_field", LongSerializer.get, FieldSerializers.Integer))
  val integerField = Field(integerId, DateTime.now(), integer = Some(integerCf))

  val floatId = FieldIdentifier("float_field", DataType.FLOAT)
  val floatCf = new TypedFieldColumnFamily(floatId.fieldName, "float_field", 0, new FloatField(),
    new ColumnFamily[java.lang.Long,DoublePosting]("float_field", LongSerializer.get, FieldSerializers.Float))
  val floatField = Field(floatId, DateTime.now(), float = Some(floatCf))

  val datetimeId = FieldIdentifier("datetime_field", DataType.DATETIME)
  val datetimeCf = new TypedFieldColumnFamily(datetimeId.fieldName, "datetime_field", 0, new DatetimeField(),
    new ColumnFamily[java.lang.Long,DatePosting]("datetime_field", LongSerializer.get, FieldSerializers.Datetime))
  val datetimeField = Field(datetimeId, DateTime.now(), datetime = Some(datetimeCf))

  val addressId = FieldIdentifier("address_field", DataType.ADDRESS)
  val addressCf = new TypedFieldColumnFamily(addressId.fieldName, "address_field", 0, new AddressField(),
    new ColumnFamily[java.lang.Long,AddressPosting]("address_field", LongSerializer.get, FieldSerializers.Address))
  val addressField = Field(addressId, DateTime.now(), address = Some(addressCf))

  val hostnameId = FieldIdentifier("hostname_field", DataType.HOSTNAME)
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
      case FieldIdentifier(name, DataType.TEXT) =>
        keyspace.createTextField(name).getResult
      case FieldIdentifier(name, DataType.LITERAL) =>
        keyspace.createLiteralField(name).getResult
      case FieldIdentifier(name, DataType.INTEGER) =>
        keyspace.createIntegerField(name).getResult
      case FieldIdentifier(name, DataType.FLOAT) =>
        keyspace.createFloatField(name).getResult
      case FieldIdentifier(name, DataType.DATETIME) =>
        keyspace.createDatetimeField(name).getResult
      case FieldIdentifier(name, DataType.ADDRESS) =>
        keyspace.createAddressField(name).getResult
      case FieldIdentifier(name, DataType.HOSTNAME) =>
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

object RequiresTestCluster extends Tag("Requires TestCluster")
