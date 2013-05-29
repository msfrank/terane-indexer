package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.bier._
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{MutationBatch, AstyanaxContext}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.{ColumnList, ColumnFamily}
import com.netflix.astyanax.serializers.{ListSerializer, SetSerializer, UUIDSerializer, StringSerializer}
import java.util.{Date, UUID}
import scala.collection.JavaConversions._
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.syntaxjockey.terane.indexer.EventRouter.GetEvent
import org.joda.time.{DateTimeZone, DateTime}
import com.syntaxjockey.terane.indexer.sink.CassandraSink.FieldColumnFamily
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import org.apache.cassandra.db.marshal.{UTF8Type, AbstractType, IntegerType, Int32Type}
import com.syntaxjockey.terane.indexer.EventRouter.GetEvent
import com.syntaxjockey.terane.indexer.sink.CassandraSink.FieldColumnFamily
import scala.Some
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import org.xbill.DNS.Name
import java.net.InetAddress

/**
 *
 */
class CassandraSink(storeName: String) extends Actor with ActorLogging {

  val config = context.system.settings.config.getConfig("terane.cassandra")

  /* connect to cassandra cluster */
  val csConfiguration = new AstyanaxConfigurationImpl()
    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
  log.debug("csConfiguration = {}", csConfiguration)
  val csPoolConfiguration = new ConnectionPoolConfigurationImpl(config.getString("connection-pool-name"))
    .setPort(config.getInt("port"))
    .setMaxConnsPerHost(config.getInt("max-conns-per-host"))
    .setSeeds(config.getStringList("seeds").mkString(","))
  log.debug("csPoolConfiguration = {}", csPoolConfiguration)
  val csConnectionPoolMonitor = new CountingConnectionPoolMonitor()
  log.debug("csConnectionPoolMonitor = {}", csConnectionPoolMonitor)
  val csContext = new AstyanaxContext.Builder()
    .forCluster(config.getString("cluster-name"))
    .forKeyspace(storeName)
    .withAstyanaxConfiguration(csConfiguration)
    .withConnectionPoolConfiguration(csPoolConfiguration)
    .withConnectionPoolMonitor(csConnectionPoolMonitor)
    .buildCluster(ThriftFamilyFactory.getInstance())
  log.debug("csContext = {}", csContext)
  csContext.start()
  val csCluster = csContext.getClient
  log.debug("csCluster = {}", csCluster)
  val csKeyspace = csCluster.getKeyspace(storeName)
  log.debug("csKeyspace = {}", csKeyspace)
  log.info("connecting to store '{}'", storeName)

  /* declare our field -> column family mappings */
  val textColumnFamilies = scala.collection.mutable.HashMap[String,FieldColumnFamily[TextField,StringPosting]]()
  val literalColumnFamilies = scala.collection.mutable.HashMap[String,FieldColumnFamily[LiteralField,StringPosting]]()
  val datetimeColumnFamilies = scala.collection.mutable.HashMap[String,FieldColumnFamily[DatetimeField,DatePosting]]()
  val hostnameColumnFamilies = scala.collection.mutable.HashMap[String,FieldColumnFamily[HostnameField,StringPosting]]()

  textColumnFamilies("message") = FieldColumnFamily(
    new TextField(),
    new ColumnFamily[StringPosting,String]("text_message", FieldSerializers.TextFieldSerializer, StringSerializer.get()))
  literalColumnFamilies("facility") = FieldColumnFamily(
    new LiteralField(),
    new ColumnFamily[StringPosting,String]("literal_facility", FieldSerializers.TextFieldSerializer, StringSerializer.get()))
  literalColumnFamilies("severity") = FieldColumnFamily(
    new LiteralField(),
    new ColumnFamily[StringPosting,String]("literal_severity", FieldSerializers.TextFieldSerializer, StringSerializer.get()))
  datetimeColumnFamilies("timestamp") = FieldColumnFamily(
    new DatetimeField(),
    new ColumnFamily[DatePosting,String]("datetime_timestamp", FieldSerializers.DatetimeFieldSerializer, StringSerializer.get()))
  hostnameColumnFamilies("origin") = FieldColumnFamily(
    new HostnameField(),
    new ColumnFamily[StringPosting,String]("hostname_origin", FieldSerializers.TextFieldSerializer, StringSerializer.get()))

  /* if the keyspace for storeName doesn't exist, then create it */
  val csKeyspaceDef = try {
    csKeyspace.describeKeyspace()
  } catch {
    case ex: BadRequestException =>
      val keyspaceOpts = new java.util.HashMap[String,Object]()
      keyspaceOpts.put("strategy_class", "SimpleStrategy")
      val strategyOpts = new java.util.HashMap[String,Object]()
      strategyOpts.put("replication_factor", "1")
      keyspaceOpts.put("strategy_options", strategyOpts)
      val cfOpts = new java.util.HashMap[ColumnFamily[_,_],java.util.Map[String,Object]]()
      cfOpts.put(CassandraSink.CF_EVENTS, null)
      /* create the keyspace with the "events" column family pre-specified */
      csKeyspace.createKeyspace(keyspaceOpts, cfOpts)
      /* create the postings column families */
      csCluster.addColumnFamily(
        csCluster.makeColumnFamilyDefinition()
          .setKeyspace(storeName)
          .setName("text_message")
          .setComparatorType("UTF8Type")
          .setDefaultValidationClass("BytesType")
          .setKeyValidationClass("CompositeType(UTF8Type, TimeUUIDType)"))
      csCluster.addColumnFamily(
        csCluster.makeColumnFamilyDefinition()
          .setKeyspace(storeName)
          .setName("literal_facility")
          .setComparatorType("UTF8Type")
          .setDefaultValidationClass("BytesType")
          .setKeyValidationClass("CompositeType(UTF8Type, TimeUUIDType)"))
      csCluster.addColumnFamily(
        csCluster.makeColumnFamilyDefinition()
          .setKeyspace(storeName)
          .setName("literal_severity")
          .setComparatorType("UTF8Type")
          .setDefaultValidationClass("BytesType")
          .setKeyValidationClass("CompositeType(UTF8Type, TimeUUIDType)"))
      csCluster.addColumnFamily(
        csCluster.makeColumnFamilyDefinition()
          .setKeyspace(storeName)
          .setName("datetime_timestamp")
          .setComparatorType("UTF8Type")
          .setDefaultValidationClass("BytesType")
          .setKeyValidationClass("CompositeType(DateType, TimeUUIDType)"))
      csCluster.addColumnFamily(
        csCluster.makeColumnFamilyDefinition()
          .setKeyspace(storeName)
          .setName("hostname_origin")
          .setComparatorType("UTF8Type")
          .setDefaultValidationClass("BytesType")
          .setKeyValidationClass("CompositeType(UTF8Type, TimeUUIDType)"))
      csKeyspace.describeKeyspace()
  }

  /* create any missing column families */
  log.debug("csKeyspaceDef = {}", csKeyspaceDef)
  for (cf <- csKeyspaceDef.getColumnFamilyList)
    log.debug("found ColumnFamily {}", cf.getName)

  def receive = {
    case event: Event =>
      writeEvent(event)
    case GetEvent(id) =>
     sender ! readEvent(id)
  }

  /**
   *
   * @param id
   * @return
   */
  def readEvent(id: UUID): Option[Event] = {
    log.debug("looking up event {}", id)
    val result = csKeyspace.prepareQuery(CassandraSink.CF_EVENTS).getKey(id).execute()
    val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val columnList: ColumnList[String] = result.getResult
    if (!columnList.isEmpty) {
      val event = new Event(id)
      log.debug("found event {} in {}", event.id, latency)
      val numColumns = columnList.size()
      log.debug("event {} has {} columns", event.id, numColumns)
      0 until numColumns foreach { n =>
        val column = columnList.getColumnByIndex(n)
        val columnName = column.getName
        val (valueType,name) = columnName.splitAt(columnName.indexOf(':'))
        valueType match {
          case "text" =>
            event.set(name.tail, column.getStringValue)
          case "literal" =>
            val literal: List[String] = column.getValue(CassandraSink.SER_LITERAL).toList
            event.set(name.tail, literal)
          case "integer" =>
            event.set(name.tail, column.getLongValue)
          case "float" =>
            event.set(name.tail, column.getDoubleValue)
          case "datetime" =>
            event.set(name.tail, new DateTime(column.getDateValue.getTime, DateTimeZone.UTC))
          case "address" =>
            event.set(name.tail, InetAddress.getByAddress(column.getByteArrayValue))
          case "hostname" =>
            event.set(name.tail, Name.fromString(column.getStringValue))
          case default =>
            log.error("failed to read column {} from event {}; unknown value type {}", columnName, id, valueType)
        }
      }
      Some(event)
    } else {
      log.debug("no such event {}", id)
      None
    }
  }

  /**
   *
   * @param event
   */
  def writeEvent(event: Event) {
    log.debug("received event {}", event.id)
    val postingsMutation = csKeyspace.prepareMutationBatch()
    val eventMutation = csKeyspace.prepareMutationBatch()
    /* write the event and postings */
    val row = eventMutation.withRow(CassandraSink.CF_EVENTS, event.id)
    for ((name,value) <- event) {
      for (text <- value.text) {
        row.putColumn("text:" + name, text)
        writeTextPosting(postingsMutation, name, text, event.id)
      }
      for (literal <- value.literal) {
        val javaLiteral: java.util.List[java.lang.String] = literal
        row.putColumn("literal:" + name, javaLiteral, CassandraSink.SER_LITERAL, new java.lang.Integer(0))
        writeLiteralPosting(postingsMutation, name, literal, event.id)
      }
      for (datetime <- value.datetime) {
        row.putColumn("datetime:" + name, datetime.toDate)
        writeDatetimePosting(postingsMutation, name, datetime, event.id)
      }
      for (hostname <- value.hostname) {
        row.putColumn("hostname:" + name, hostname.toString)
        writeHostnamePosting(postingsMutation, name, hostname, event.id)
      }
    }
    /* execute the event mutation */
    try {
      val result = eventMutation.execute()
      val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      log.debug("wrote event {} in {}", event.id, latency)
    } catch {
      case ex: Exception =>
        log.error(ex, "failed to write event {}", event.id)
    }
    /* execute the postings mutations */
    try {
      val result = postingsMutation.execute()
      val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      log.debug("wrote postings for {} in {}", event.id, latency)
    } catch {
      case ex: Exception =>
        log.error(ex, "failed to write postings for {}", event.id)
    }
  }

  /**
   *
   * @param mutation
   * @param name
   * @param text
   * @param id
   */
  def writeTextPosting(mutation: MutationBatch, name: String, text: String, id: UUID) {
    textColumnFamilies.get(name) match {
      case Some(FieldColumnFamily(field: TextField, cf: ColumnFamily[StringPosting,String])) =>
        val postings: Seq[(String,PostingMetadata)] = field.parseValue(text)
        for ((term,postingMetadata) <- postings) {
          val row = mutation.withRow(cf, new StringPosting(term, id))
          val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
            pos:java.lang.Integer
          }
          val ttl = new java.lang.Integer(0)
          row.putColumn("positions", positions, CassandraSink.SER_POSITIONS, ttl)
          log.debug("wrote posting %s,%s to batch".format(term, id))
        }
      case None =>
        log.warning("no column family '%s' defined for type text".format(name))
    }
  }

  /**
   *
   * @param mutation
   * @param name
   * @param literal
   * @param id
   */
  def writeLiteralPosting(mutation: MutationBatch, name: String, literal: List[String], id: UUID) {
     literalColumnFamilies.get(name) match {
      case Some(FieldColumnFamily(field: LiteralField, cf: ColumnFamily[StringPosting,String])) =>
        val postings: Seq[(String,PostingMetadata)] = field.parseValue(literal)
        for ((term,postingMetadata) <- postings) {
          val row = mutation.withRow(cf, new StringPosting(term, id))
          val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
            pos:java.lang.Integer
          }
          val ttl = new java.lang.Integer(0)
          row.putColumn("positions", positions, CassandraSink.SER_POSITIONS, ttl)
          log.debug("wrote posting %s,%s to batch".format(term, id))
        }
      case None =>
        log.warning("no column family '%s' defined for type literal".format(name))
    }
  }

  /**
   *
   * @param mutation
   * @param name
   * @param datetime
   * @param id
   */
  def writeDatetimePosting(mutation: MutationBatch, name: String, datetime: DateTime, id: UUID) {
    datetimeColumnFamilies.get(name) match {
      case Some(FieldColumnFamily(field: DatetimeField, cf: ColumnFamily[DatePosting,String])) =>
        val postings: Seq[(Date,PostingMetadata)] = field.parseValue(datetime)
        for ((term,postingMetadata) <- postings) {
          val row = mutation.withRow(cf, new DatePosting(term, id))
          val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
            pos:java.lang.Integer
          }
          val ttl = new java.lang.Integer(0)
          row.putColumn("positions", positions, CassandraSink.SER_POSITIONS, ttl)
          log.debug("wrote posting %s,%s to batch".format(term, id))
        }
      case None =>
        log.warning("no column family '%s' defined for type datetime".format(name))
    }
  }

  /**
   *
   * @param mutation
   * @param name
   * @param hostname
   * @param id
   */
  def writeHostnamePosting(mutation: MutationBatch, name: String, hostname: Name, id: UUID) {
     hostnameColumnFamilies.get(name) match {
      case Some(FieldColumnFamily(field: HostnameField, cf: ColumnFamily[StringPosting,String])) =>
        val postings: Seq[(String,PostingMetadata)] = field.parseValue(hostname)
        for ((term,postingMetadata) <- postings) {
          val row = mutation.withRow(cf, new StringPosting(term, id))
          val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
            pos:java.lang.Integer
          }
          val ttl = new java.lang.Integer(0)
          row.putColumn("positions", positions, CassandraSink.SER_POSITIONS, ttl)
          log.debug("wrote posting %s,%s to batch".format(term, id))
        }
      case None =>
        log.warning("no column family '%s' defined for type hostname".format(name))
    }
  }
}

object CassandraSink {
  val CF_EVENTS = new ColumnFamily[UUID,String]("events", UUIDSerializer.get(), StringSerializer.get())
  val SER_POSITIONS = new SetSerializer[java.lang.Integer](Int32Type.instance)
  val SER_LITERAL = new ListSerializer[java.lang.String](UTF8Type.instance)
  case class FieldColumnFamily[F,P](field: F, cf: ColumnFamily[P,String])
}
