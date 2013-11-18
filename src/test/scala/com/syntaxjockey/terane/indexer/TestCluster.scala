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

import org.scalatest.{Suite, BeforeAndAfterAll, Tag}
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.Config
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.LongSerializer
import com.netflix.astyanax.{Cluster, Keyspace}
import org.joda.time.DateTime
import scala.Some

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.cassandra._
import com.syntaxjockey.terane.indexer.zookeeper._
import com.syntaxjockey.terane.indexer.sink.CassandraField

abstract class TestCluster(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with Suite with BeforeAndAfterAll {
  import TestCluster._

  def this(actorSystemName: String, config: Config) = this(ActorSystem(actorSystemName, config))

  def this(actorSystemName: String, composableConfig: ComposableConfig) = this(ActorSystem(actorSystemName, composableConfig.config))

  def this(actorSystemName: String, mergedConfig: MergedConfig) = this(ActorSystem(actorSystemName, mergedConfig.config))

  def this(actorSystemName: String) = this(ActorSystem(actorSystemName, (AkkaConfig ++ CassandraConfig ++ ZookeeperConfig).config))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val textId = FieldIdentifier("text_field", DataType.TEXT)
  val textCf = TextFieldCF(textId.fieldName, "text_field", 0, new TextField(),
    new ColumnFamily[java.lang.Long,StringPosting]("text_field", LongSerializer.get, Serializers.Text),
    None, None)
  val textField = CassandraField(textId, DateTime.now(), text = Some(textCf))

  val literalId = FieldIdentifier("literal_field", DataType.LITERAL)
  val literalCf = LiteralFieldCF(literalId.fieldName, "literal_field", 0, new LiteralField(),
    new ColumnFamily[java.lang.Long,StringPosting]("literal_field", LongSerializer.get, Serializers.Literal),
    None, None)
  val literalField = CassandraField(literalId, DateTime.now(), literal = Some(literalCf))

  val integerId = FieldIdentifier("integer_field", DataType.INTEGER)
  val integerCf = IntegerFieldCF(integerId.fieldName, "integer_field", 0, new IntegerField(),
    new ColumnFamily[java.lang.Long,LongPosting]("integer_field", LongSerializer.get, Serializers.Integer),
    None)
  val integerField = CassandraField(integerId, DateTime.now(), integer = Some(integerCf))

  val floatId = FieldIdentifier("float_field", DataType.FLOAT)
  val floatCf = FloatFieldCF(floatId.fieldName, "float_field", 0, new FloatField(),
    new ColumnFamily[java.lang.Long,DoublePosting]("float_field", LongSerializer.get, Serializers.Float),
    None)
  val floatField = CassandraField(floatId, DateTime.now(), float = Some(floatCf))

  val datetimeId = FieldIdentifier("datetime_field", DataType.DATETIME)
  val datetimeCf = DatetimeFieldCF(datetimeId.fieldName, "datetime_field", 0, new DatetimeField(),
    new ColumnFamily[java.lang.Long,DatePosting]("datetime_field", LongSerializer.get, Serializers.Datetime),
    None)
  val datetimeField = CassandraField(datetimeId, DateTime.now(), datetime = Some(datetimeCf))

  val addressId = FieldIdentifier("address_field", DataType.ADDRESS)
  val addressCf = AddressFieldCF(addressId.fieldName, "address_field", 0, new AddressField(),
    new ColumnFamily[java.lang.Long,AddressPosting]("address_field", LongSerializer.get, Serializers.Address),
    None)
  val addressField = CassandraField(addressId, DateTime.now(), address = Some(addressCf))

  val hostnameId = FieldIdentifier("hostname_field", DataType.HOSTNAME)
  val hostnameCf = HostnameFieldCF(hostnameId.fieldName, "hostname_field", 0, new HostnameField(),
    new ColumnFamily[java.lang.Long,StringPosting]("hostname_field", LongSerializer.get, Serializers.Hostname),
    None)
  val hostnameField = CassandraField(hostnameId, DateTime.now(), hostname = Some(hostnameCf))

  def getZookeeperClient = Zookeeper(_system).client

  def getCassandraClient = Cassandra(_system).cluster

  /**
   *
   * @param cluster
   * @param keyspaceName
   * @return
   */
  def createKeyspace(cluster: Cluster, keyspaceName: String): Keyspace = {
    cluster.createKeyspace(keyspaceName).getResult
    val _keyspace = new KeyspaceWithCFOperations(cluster.getKeyspace(keyspaceName))
    _keyspace.keyspace
  }

  /**
   *
   * @param keyspace
   * @param field
   */
  def createColumnFamily(keyspace: Keyspace, field: CassandraField) {
    field.fieldId match {
      case FieldIdentifier(name, DataType.TEXT) =>
        keyspace.createTextField(field.text.get)
      case FieldIdentifier(name, DataType.LITERAL) =>
        keyspace.createLiteralField(field.literal.get)
      case FieldIdentifier(name, DataType.INTEGER) =>
        keyspace.createIntegerField(field.integer.get)
      case FieldIdentifier(name, DataType.FLOAT) =>
        keyspace.createFloatField(field.float.get)
      case FieldIdentifier(name, DataType.DATETIME) =>
        keyspace.createDatetimeField(field.datetime.get)
      case FieldIdentifier(name, DataType.ADDRESS) =>
        keyspace.createAddressField(field.address.get)
      case FieldIdentifier(name, DataType.HOSTNAME) =>
        keyspace.createHostnameField(field.hostname.get)
    }
  }
}

object TestCluster {
  import scala.language.implicitConversions

  class ClusterWithKeyspaceOperations(val cluster: Cluster) extends CassandraKeyspaceOperations {}
  class KeyspaceWithCFOperations(val keyspace: Keyspace) extends CassandraCFOperations {}
  class KeyspaceWithRowOperations(val keyspace: Keyspace) extends CassandraRowOperations {}

  implicit def cluster2KeyspaceOperations(cluster: Cluster): ClusterWithKeyspaceOperations = new ClusterWithKeyspaceOperations(cluster)
  implicit def keyspace2CFOperations(keyspace: Keyspace): KeyspaceWithCFOperations = new KeyspaceWithCFOperations(keyspace)
  implicit def keyspace2RowOperations(keyspace: Keyspace): KeyspaceWithRowOperations = new KeyspaceWithRowOperations(keyspace)
}

object RequiresTestCluster extends Tag("Requires TestCluster")
