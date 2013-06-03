package com.syntaxjockey.terane.indexer.sink

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.netflix.astyanax.{Cluster, Keyspace, MutationBatch}
import java.util.{Date, UUID}
import akka.event.LoggingAdapter

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.sink.FieldManager.{FieldColumnFamily, TypedFieldColumnFamily}
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata

import scala.collection.JavaConversions._
import org.joda.time.DateTime
import org.xbill.DNS.Name

trait EventWriter extends FieldManager {

  def log: LoggingAdapter
  def csKeyspace: Keyspace
  def csCluster: Cluster

  /**
   *
   * @param event
   */
  def writeEvent(event: Event) {
    log.debug("received event {}", event.id)
    /* create our batches */
    val postingsMutation = csKeyspace.prepareMutationBatch()
    val eventMutation = csKeyspace.prepareMutationBatch()
    /* write the event */
    val row = eventMutation.withRow(CassandraSink.CF_EVENTS, event.id)
    /* write the postings */
    for ((name,value) <- event) {
      for (text <- value.text) {
        val fcf = getOrCreateTextField(name)
        row.putColumn(fcf.id, text)
        writeTextPosting(postingsMutation, fcf, text, event.id)
      }
      for (literal <- value.literal) {
        val fcf = getOrCreateLiteralField(name)
        val javaLiteral: java.util.List[java.lang.String] = literal
        row.putColumn(fcf.id, javaLiteral, CassandraSink.SER_LITERAL, new java.lang.Integer(0))
        writeLiteralPosting(postingsMutation, fcf, literal, event.id)
      }
      for (datetime <- value.datetime) {
        val fcf = getOrCreateDatetimeField(name)
        row.putColumn(fcf.id, datetime.toDate)
        writeDatetimePosting(postingsMutation, fcf, datetime, event.id)
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

  def getShardKey(id: UUID, fcf: FieldColumnFamily): java.lang.Long = {
    val lsb: Long = id.getMostSignificantBits
    val mask: Long = 0xffffffffffffffffL >>> (64 - fcf.width)
    lsb & mask
    // FIXME: return the actual shard key
    0
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param text
   * @param id
   */
  def writeTextPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[TextField,StringPosting], text: String, id: UUID) {
    val postings: Seq[(String,PostingMetadata)] = fcf.field.parseValue(text)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new StringPosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
      log.debug("wrote posting %d:%s,%s to batch".format(shard.toLong, term, id))
    }
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param literal
   * @param id
   */
  def writeLiteralPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[LiteralField,StringPosting], literal: List[String], id: UUID) {
    val postings: Seq[(String,PostingMetadata)] = fcf.field.parseValue(literal)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new StringPosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
      log.debug("wrote posting %d:%s,%s to batch".format(shard, term, id))
    }
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param datetime
   * @param id
   */
  def writeDatetimePosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[DatetimeField,DatePosting], datetime: DateTime, id: UUID) {
    val postings: Seq[(Date,PostingMetadata)] = fcf.field.parseValue(datetime)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new DatePosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
      log.debug("wrote posting %d:%s,%s to batch".format(shard, term, id))
    }
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param hostname
   * @param id
   */
  def writeHostnamePosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[HostnameField,StringPosting], hostname: Name, id: UUID) {
    val postings: Seq[(String,PostingMetadata)] = fcf.field.parseValue(hostname)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new StringPosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
      log.debug("wrote posting %d:%s,%s to batch".format(shard, term, id))
    }
  }
}
