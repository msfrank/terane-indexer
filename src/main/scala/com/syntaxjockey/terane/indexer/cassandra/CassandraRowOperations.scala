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

package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.{MutationBatch, Keyspace}
import scala.collection.JavaConversions._
import java.util.{Date, UUID}

import com.syntaxjockey.terane.indexer.sink.FieldManager.TypedFieldColumnFamily
import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.sink._
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
  def writeTextPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[TextField,StringPosting], text: Text, id: UUID) {
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
  def writeLiteralPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[LiteralField,StringPosting], literal: Literal, id: UUID) {
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
  def writeIntegerPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[IntegerField,LongPosting], integer: Integer, id: UUID) {
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
  def writeFloatPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[FloatField,DoublePosting], float: Float, id: UUID) {
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
  def writeDatetimePosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[DatetimeField,DatePosting], datetime: Datetime, id: UUID) {
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
  def writeAddressPosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[AddressField,AddressPosting], address: Address, id: UUID) {
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
  def writeHostnamePosting(mutation: MutationBatch, fcf: TypedFieldColumnFamily[HostnameField,StringPosting], hostname: Hostname, id: UUID) {
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
