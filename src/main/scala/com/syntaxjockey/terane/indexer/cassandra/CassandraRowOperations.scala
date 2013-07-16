package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.{MutationBatch, Keyspace}
import scala.collection.JavaConversions._
import java.util.{Date, UUID}

import com.syntaxjockey.terane.indexer.sink.FieldManager.{FieldColumnFamily, TypedFieldColumnFamily}
import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import org.joda.time.DateTime
import java.net.InetAddress
import org.xbill.DNS.Name
import com.syntaxjockey.terane.indexer.sink._
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldColumnFamily
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.sink.FieldManager.FieldColumnFamily
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata

trait CassandraRowOperations {

  implicit val keyspace: Keyspace

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
    }
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param integer
   * @param id
   */
  def writeIntegerPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[IntegerField,LongPosting], integer: Long, id: UUID) {
    val postings: Seq[(Long,PostingMetadata)] = fcf.field.parseValue(integer)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new LongPosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
    }
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param float
   * @param id
   */
  def writeFloatPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[FloatField,DoublePosting], float: Double, id: UUID) {
    val postings: Seq[(Double,PostingMetadata)] = fcf.field.parseValue(float)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new DoublePosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
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
    }
  }

  /**
   *
   * @param mutation
   * @param fcf
   * @param address
   * @param id
   */
  def writeAddressPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[AddressField,AddressPosting], address: InetAddress, id: UUID) {
    val postings: Seq[(Array[Byte],PostingMetadata)] = fcf.field.parseValue(address)
    for ((term,postingMetadata) <- postings) {
      val positions: java.util.Set[java.lang.Integer] = postingMetadata.positions.getOrElse(Set[Int]()).map { pos =>
        pos:java.lang.Integer
      }
      val shard = getShardKey(id, fcf)
      val ttl = new java.lang.Integer(0)
      mutation.withRow(fcf.cf, shard).putColumn(new AddressPosting(term, id), positions, CassandraSink.SER_POSITIONS, ttl)
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
    }
  }

  /**
   * extract the shard key from the event id.
   *
   * @param id
   * @param fcf
   * @return
   */
  def getShardKey(id: UUID, fcf: FieldColumnFamily): java.lang.Long = {
    val lsb: Long = id.getMostSignificantBits
    val mask: Long = 0xffffffffffffffffL >>> (64 - fcf.width)
    lsb & mask
    // FIXME: return the actual shard key
    0
  }
}
