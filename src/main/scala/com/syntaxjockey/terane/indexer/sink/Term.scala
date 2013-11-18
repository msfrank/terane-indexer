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

package com.syntaxjockey.terane.indexer.sink

import akka.actor._
import akka.pattern.ask
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.util.Timeout
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.Column
import com.netflix.astyanax.query.ColumnQuery
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.Some
import scala.collection.JavaConversions._
import java.util.{Date, UUID}

import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.datatypes.DataType
import com.syntaxjockey.terane.indexer.bier.statistics.{Analytical, FieldStatistics}
import com.syntaxjockey.terane.indexer.bier.Matchers
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, MatchResult, NextPosting, FindPosting}
import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata
import com.syntaxjockey.terane.indexer.cassandra._

/**
 *
 */
case class Term[T](fieldId: FieldIdentifier, term: T, keyspace: Keyspace, field: CassandraField, stats: Option[Agent[FieldStatistics]])(implicit val factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps
  import Analytical._

  implicit val timeout = Timeout(5 seconds)

  // FIXME: create multiple iterators for each shard
  lazy val iterator = factory.actorOf(Props(classOf[TermIterator[T]], this, 0))

  def estimateCost: Long = {
    if (stats.isDefined) {
      val _stats = stats.get.get()
      term match {
        case v: String =>
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case v: Long =>
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case v: Double =>
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case v: Date =>
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case v: Array[Byte] =>
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
      }
    } else 0
  }

  def nextPosting: Future[MatchResult] = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID): Future[MatchResult] = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }

  def hashString: String = "%s:%s[%s]=\"%s\"".format(this.getClass.getName, fieldId.fieldName, fieldId.fieldType.toString.toLowerCase, term.toString)
}

class TermIterator[T](term: Term[T], shard: Int) extends Actor with ActorLogging {
  import Matchers._

  val scanner = makeScanner(term, shard)
  var postings: List[BierPosting] = List.empty

  def receive = LoggingReceive {

    case NextPosting if postings.isEmpty =>
      postings = scanner.execute().getResult.map(makePosting).toList
      postings = if (!postings.isEmpty) {
        sender ! Right(postings.head)
        postings.tail
      } else {
        sender ! Left(NoMoreMatches)
        postings
      }

    case NextPosting =>
      sender ! Right(postings.head)
      postings = postings.tail

    case FindPosting(id) =>
      try {
        val result = findPosting(id, term, shard).execute().getResult
        sender ! Right(makePosting(result))
      } catch {
        case ex: NotFoundException => sender ! Left(NoMoreMatches)
      }
  }

  /**
   *
   * @param term
   * @param shard
   * @return
   */
  def makeScanner(term: Term[T], shard: Int) = {
    term match {
      case Term(FieldIdentifier(_, DataType.TEXT), text: String, _, _, _) =>
        val range = Serializers.Text.buildRange().greaterThanEquals(text).lessThanEquals(text).build()
        term.keyspace.prepareQuery(term.field.text.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case Term(FieldIdentifier(_, DataType.LITERAL), literal: String, _, _, _) =>
        val range = Serializers.Literal.buildRange().greaterThanEquals(literal).lessThanEquals(literal).build()
        term.keyspace.prepareQuery(term.field.literal.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case Term(FieldIdentifier(_, DataType.INTEGER), integer: Long, _, _, _) =>
        val range = Serializers.Integer.buildRange().greaterThanEquals(integer).lessThanEquals(integer).build()
        term.keyspace.prepareQuery(term.field.integer.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case Term(FieldIdentifier(_, DataType.FLOAT), float: Double, _, _, _) =>
        val range = Serializers.Float.buildRange().greaterThanEquals(float).lessThanEquals(float).build()
        term.keyspace.prepareQuery(term.field.float.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case Term(FieldIdentifier(_, DataType.DATETIME), datetime: Date, _, _, _) =>
        val range = Serializers.Datetime.buildRange().greaterThanEquals(datetime).lessThanEquals(datetime).build()
        term.keyspace.prepareQuery(term.field.datetime.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case Term(FieldIdentifier(_, DataType.ADDRESS), address: Array[Byte], _, _, _) =>
        val range = Serializers.Address.buildRange().greaterThanEquals(address).lessThanEquals(address).build()
        term.keyspace.prepareQuery(term.field.address.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case Term(FieldIdentifier(_, DataType.HOSTNAME), hostname: String, _, _, _) =>
        val range = Serializers.Hostname.buildRange().greaterThanEquals(hostname).lessThanEquals(hostname).build()
        term.keyspace.prepareQuery(term.field.hostname.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case unknown => throw new Exception("can't make Term scanner for " + term.toString)
    }
  }

  /**
   *
   * @param id
   * @param term
   * @param shard
   * @return
   */
  def findPosting(id: UUID, term: Term[T], shard: Int): ColumnQuery[_] = {
    term match {
      case Term(FieldIdentifier(_, DataType.TEXT), text: String, _, _, _) =>
        term.keyspace.prepareQuery(term.field.text.get.terms)
          .getKey(shard)
          .getColumn(new StringPosting(text, id))
      case Term(FieldIdentifier(_, DataType.LITERAL), literal: String, _, _, _) =>
        term.keyspace.prepareQuery(term.field.literal.get.terms)
          .getKey(shard)
          .getColumn(new StringPosting(literal, id))
      case Term(FieldIdentifier(_, DataType.INTEGER), integer: Long, _, _, _) =>
        term.keyspace.prepareQuery(term.field.integer.get.terms)
          .getKey(shard)
          .getColumn(new LongPosting(integer, id))
      case Term(FieldIdentifier(_, DataType.FLOAT), float: Double, _, _, _) =>
        term.keyspace.prepareQuery(term.field.float.get.terms)
          .getKey(shard)
          .getColumn(new DoublePosting(float, id))
      case Term(FieldIdentifier(_, DataType.DATETIME), datetime: Date, _, _, _) =>
        term.keyspace.prepareQuery(term.field.datetime.get.terms)
          .getKey(shard)
          .getColumn(new DatePosting(datetime, id))
      case Term(FieldIdentifier(_, DataType.ADDRESS), address: Array[Byte], _, _, _) =>
        term.keyspace.prepareQuery(term.field.address.get.terms)
          .getKey(shard)
          .getColumn(new AddressPosting(address, id))
      case Term(FieldIdentifier(_, DataType.HOSTNAME), hostname: String, _, _, _) =>
        term.keyspace.prepareQuery(term.field.hostname.get.terms)
          .getKey(shard)
          .getColumn(new StringPosting(hostname, id))
      case unknown => throw new Exception("can't make Term finder for " + term.toString)
    }
  }

  /**
   *
   * @param column
   * @return
   */
  def makePosting(column: Column[_]): BierPosting = {
    val positions = column.getValue(CassandraSink.SER_POSITIONS) map { i => i.toInt }
    val id = column.getName match {
      case p: StringPosting => p.id
      case p: LongPosting => p.id
      case p: DoublePosting => p.id
      case p: DatePosting => p.id
      case p: AddressPosting => p.id
    }
    BierPosting(id, PostingMetadata(Some(positions)))
  }
}