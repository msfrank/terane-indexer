package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.bier._
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{ListSerializer, SetSerializer, UUIDSerializer}
import java.util.UUID
import scala.collection.JavaConversions._
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException
import org.apache.cassandra.db.marshal.{UTF8Type, Int32Type}
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata

/**
 *
 */
class CassandraSink(storeName: String) extends Actor with ActorLogging with EventReader with EventWriter with EventSearcher {
  import com.syntaxjockey.terane.indexer.EventRouter._

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
      getOrCreateTextField("message")
      getOrCreateLiteralField("facility")
      getOrCreateLiteralField("severity")
      getOrCreateDatetimeField("timestamp")
      /* get the keyspace definition */
      csKeyspace.describeKeyspace()
  }

  log.debug("csKeyspaceDef = {}", csKeyspaceDef)
  for (cf <- csKeyspaceDef.getColumnFamilyList)
    log.debug("found ColumnFamily {}", cf.getName)

  def receive = {
    case event: Event =>
      writeEvent(event)
    case matchers: Matchers =>
      // FIXME: use future here
      val optimized = matchers.optimizeMatcher(this)
      val postings: List[UUID] = optimized.take(10).map(p => p._1).toList
      val events = getEvents(postings)
      sender ! events
  }
}

object CassandraSink {
  val CF_EVENTS = new ColumnFamily[UUID,UUID]("events", UUIDSerializer.get(), UUIDSerializer.get())
  val SER_POSITIONS = new SetSerializer[java.lang.Integer](Int32Type.instance)
  val SER_LITERAL = new ListSerializer[java.lang.String](UTF8Type.instance)
}
