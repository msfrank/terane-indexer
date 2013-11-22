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

import com.syntaxjockey.terane.indexer.bier.{MatchTerm, FieldIdentifier, Matchers}
import com.syntaxjockey.terane.indexer.bier.datatypes.DataType
import com.syntaxjockey.terane.indexer.bier.statistics.{Analytical, FieldStatistics}
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, MatchResult, NextPosting, FindPosting}
import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata
import com.syntaxjockey.terane.indexer.cassandra._

/**
 * Match the term of the specified type in the specified field.
 */
case class Term(fieldId: FieldIdentifier, term: MatchTerm, keyspace: Keyspace, field: CassandraField, stats: Option[Agent[FieldStatistics]])(implicit val factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps
  import Analytical._

  implicit val timeout = Timeout(5 seconds)

  // FIXME: create multiple iterators for each shard
  lazy val iterator = factory.actorOf(Props(classOf[TermIterator], this, 0))

  def estimateCost: Long = {
    if (stats.isDefined) {
      val _stats = stats.get.get()
      fieldId.fieldType match {
        case DataType.TEXT =>
          val v = term.text.get
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case DataType.LITERAL =>
          val v = term.literal.get
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case DataType.INTEGER =>
          val v = term.integer.get
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case DataType.FLOAT =>
          val v = term.float.get
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case DataType.DATETIME =>
          val v = term.datetime.get
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case DataType.ADDRESS =>
          val v = term.address.get
          if (_stats.estimateTermSetContains(v).isTrue) _stats.estimateTermFrequency(v).estimate else 0
        case DataType.HOSTNAME =>
          val v = term.hostname.get
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

/**
 *
 */
class TermIterator(term: Term, shard: Int) extends Actor with ActorLogging {
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
   */
  def makeScanner(term: Term, shard: Int) = {
    term.fieldId.fieldType match {
      case DataType.TEXT =>
        val range = Serializers.Text.buildRange().greaterThanEquals(term.term.text.get).lessThanEquals(term.term.text.get).build()
        term.keyspace.prepareQuery(term.field.text.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case DataType.LITERAL =>
        val range = Serializers.Literal.buildRange().greaterThanEquals(term.term.literal.get).lessThanEquals(term.term.literal.get).build()
        term.keyspace.prepareQuery(term.field.literal.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case DataType.INTEGER =>
        val range = Serializers.Integer.buildRange().greaterThanEquals(term.term.integer.get).lessThanEquals(term.term.integer.get).build()
        term.keyspace.prepareQuery(term.field.integer.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case DataType.FLOAT =>
        val range = Serializers.Float.buildRange().greaterThanEquals(term.term.float.get).lessThanEquals(term.term.float.get).build()
        term.keyspace.prepareQuery(term.field.float.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case DataType.DATETIME =>
        val range = Serializers.Datetime.buildRange().greaterThanEquals(term.term.datetime.get).lessThanEquals(term.term.datetime.get).build()
        term.keyspace.prepareQuery(term.field.datetime.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case DataType.ADDRESS =>
        val range = Serializers.Address.buildRange().greaterThanEquals(term.term.address.get).lessThanEquals(term.term.address.get).build()
        term.keyspace.prepareQuery(term.field.address.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case DataType.HOSTNAME =>
        val range = Serializers.Hostname.buildRange().greaterThanEquals(term.term.hostname.get).lessThanEquals(term.term.hostname.get).build()
        term.keyspace.prepareQuery(term.field.hostname.get.terms)
          .getKey(shard)
          .withColumnRange(range)
          .autoPaginate(true)
      case unknown => throw new Exception("can't make Term scanner for " + term.toString)
    }
  }

  /**
   *
   */
  def findPosting(id: UUID, term: Term, shard: Int): ColumnQuery[_] = {
    term.fieldId.fieldType match {
      case DataType.TEXT =>
        term.keyspace.prepareQuery(term.field.text.get.terms)
          .getKey(shard)
          .getColumn(new StringPosting(term.term.text.get, id))
      case DataType.LITERAL =>
        term.keyspace.prepareQuery(term.field.literal.get.terms)
          .getKey(shard)
          .getColumn(new StringPosting(term.term.literal.get, id))
      case DataType.INTEGER =>
        term.keyspace.prepareQuery(term.field.integer.get.terms)
          .getKey(shard)
          .getColumn(new LongPosting(term.term.integer.get, id))
      case DataType.FLOAT =>
        term.keyspace.prepareQuery(term.field.float.get.terms)
          .getKey(shard)
          .getColumn(new DoublePosting(term.term.float.get, id))
      case DataType.DATETIME =>
        term.keyspace.prepareQuery(term.field.datetime.get.terms)
          .getKey(shard)
          .getColumn(new DatePosting(term.term.datetime.get, id))
      case DataType.ADDRESS =>
        term.keyspace.prepareQuery(term.field.address.get.terms)
          .getKey(shard)
          .getColumn(new AddressPosting(term.term.address.get, id))
      case DataType.HOSTNAME =>
        term.keyspace.prepareQuery(term.field.hostname.get.terms)
          .getKey(shard)
          .getColumn(new StringPosting(term.term.hostname.get, id))
      case unknown => throw new Exception("can't make Term finder for " + term.toString)
    }
  }

  /**
   *
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