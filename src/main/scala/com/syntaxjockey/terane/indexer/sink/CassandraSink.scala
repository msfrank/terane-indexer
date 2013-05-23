package com.syntaxjockey.terane.indexer.sink

import akka.actor.{Actor, ActorLogging}
import com.syntaxjockey.terane.indexer.bier.Event
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.{MutationBatch, AstyanaxContext}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{UUIDSerializer, StringSerializer}
import java.util.UUID
import scala.collection.JavaConversions._
import org.apache.cassandra.exceptions.InvalidRequestException
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException
import scala.collection.mutable
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

/**
 *
 */
class CassandraSink(storeName: String) extends Actor with ActorLogging {

  val config = context.system.settings.config.getConfig("terane.cassandra")

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
    .buildKeyspace(ThriftFamilyFactory.getInstance())
  log.debug("csContext = {}", csContext)
  csContext.start()
  val csKeyspace = csContext.getClient
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
      csKeyspace.createKeyspace(keyspaceOpts, cfOpts)
      csKeyspace.describeKeyspace()
  }

  log.debug("csKeyspaceDef = {}", csKeyspaceDef)
  for (cf <- csKeyspaceDef.getColumnFamilyList)
    log.debug("found ColumnFamily {}", cf.getName)

  def receive = {
    case event: Event =>
      writeEvent(event)
  }

  /**
   *
   * @param event
   */
  def writeEvent(event: Event) {
    log.debug("received event {}", event.id)
    val batch = csKeyspace.prepareMutationBatch()
    val row = batch.withRow(CassandraSink.CF_EVENTS, event.id)
    for ((name,value) <- event) {
      for (text <- value.text)
        row.putColumn("text:" + name, text)
      //for (literal <- value.literal)
      //  row.putColumn[UTF8Type]("literal:" + name, literal, new SetSerializer[String](UTF8Type.instance), null)
      for (integer <- value.integer)
        row.putColumn("integer:" + name, integer)
      for (float <- value.float)
        row.putColumn("float:" + name, float)
      for (datetime <- value.datetime)
        row.putColumn("datetime:" + name, datetime.toDate)
      //for (v <- value.address)
      //
      //for (v <- value.hostname)
      //
    }
    try {
      val result = batch.execute()
      val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      log.debug("wrote event {} in {}", event.id, latency)
    } catch {
      case ex: Exception =>
        log.error(ex, "failed to write event {}", event.id)
    }
  }
}

object CassandraSink {

  val CF_EVENTS = new ColumnFamily[UUID,String]( "events", UUIDSerializer.get(), StringSerializer.get())


}
