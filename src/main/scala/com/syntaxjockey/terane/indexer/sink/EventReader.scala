package com.syntaxjockey.terane.indexer.sink

import akka.event.LoggingAdapter
import com.netflix.astyanax.{Cluster, Keyspace}
import java.util.UUID
import com.syntaxjockey.terane.indexer.bier._
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.netflix.astyanax.model.ColumnList
import org.joda.time.{DateTimeZone, DateTime}
import java.net.InetAddress
import org.xbill.DNS.Name

import scala.collection.JavaConversions._
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldColumnFamily
import scala.Some

trait EventReader extends FieldManager {

  def log: LoggingAdapter
  def csKeyspace: Keyspace
  def csCluster: Cluster

  /*
  def getEvent(id: UUID): Option[Event] = {
    log.debug("looking up event {}", id)
    val result = csKeyspace.prepareQuery(CassandraSink.CF_EVENTS).getKey(id).execute()
    val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    log.debug("getEvent took {}", latency)
    val columnList = result.getResult
    if (!columnList.isEmpty) Some(readEvent(id, columnList)) else None
  }

  def readEvent(id: UUID, columnList: ColumnList[String]): Event = {
    val event = new Event(id)
    val numColumns = columnList.size()
    log.debug("event {} has {} columns", event.id, numColumns)
    0 until numColumns foreach { n =>
      val column = columnList.getColumnByIndex(n)
      getField(column.getName) match {
        case Some((f: TextField, FieldColumnFamily(name, _, _))) =>
          event.set(name, column.getStringValue)
        case Some((f: LiteralField, FieldColumnFamily(name, _, _))) =>
          val literal: List[String] = column.getValue(CassandraSink.SER_LITERAL).toList
          event.set(name, literal)
        case Some((f: IntegerField, FieldColumnFamily(name, _, _))) =>
          event.set(name, column.getLongValue)
        case Some((f: FloatField, FieldColumnFamily(name, _, _))) =>
          event.set(name, column.getDoubleValue)
        case Some((f: DatetimeField, FieldColumnFamily(name, _, _))) =>
          event.set(name, new DateTime(column.getDateValue.getTime, DateTimeZone.UTC))
        case Some((f: AddressField, FieldColumnFamily(name, _, _))) =>
          event.set(name, InetAddress.getByAddress(column.getByteArrayValue))
        case Some((f: HostnameField, FieldColumnFamily(name, _, _))) =>
          event.set(name, Name.fromString(column.getStringValue))
        case None =>
          log.error("failed to read column {} from event {}; no such field", column.getName, id)
        case default =>
          log.error("failed to read column {} from event {}; unknown value type", column.getName, id)
      }
    }
    event
  }
  */
}
