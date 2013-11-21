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

import akka.actor.{ActorLogging, Actor, Props, ActorRefFactory}
import akka.pattern.ask
import akka.agent.Agent
import akka.event.LoggingReceive
import akka.util.Timeout
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.{ByteBufferRange, Column}
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.collection.JavaConversions._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.{MatchTerm, Matchers, FieldIdentifier}
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, MatchResult, NextPosting, FindPosting}
import com.syntaxjockey.terane.indexer.bier.matchers.RangeSpec
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics
import com.syntaxjockey.terane.indexer.cassandra._

/**
 *
 */
case class Range(fieldId: FieldIdentifier, spec: RangeSpec, keyspace: Keyspace, field: CassandraField, stats: Option[Agent[FieldStatistics]])(implicit val factory: ActorRefFactory) extends Matchers {
  import scala.language.postfixOps

  implicit val timeout = Timeout(5 seconds)

  // FIXME: create multiple iterators for each shard
  lazy val iterator = factory.actorOf(Props(classOf[RangeIterator], this, 0))

  def estimateCost: Long = if (stats.isDefined) stats.get.get().getTermCount else 0

  def nextPosting: Future[MatchResult] = iterator.ask(NextPosting).mapTo[MatchResult]

  def findPosting(id: UUID): Future[MatchResult] = iterator.ask(FindPosting(id)).mapTo[MatchResult]

  def close() {
    factory.stop(iterator)
  }

  def hashString: String = "%s:%s[%s]=%s\"%s\",\"%s\"%s".format(this.getClass.getName, fieldId.fieldName, fieldId.fieldType.toString.toLowerCase,
    if (spec.leftExcl) "{" else "[",
    spec.left.getOrElse("").toString,
    spec.right.getOrElse("").toString,
    if (spec.rightExcl) "}" else "]")
}

/**
 *
 */
class RangeIterator(range: Range, shard: Int) extends Actor with ActorLogging {
  import Matchers._

  val comparator = new RangeComparator(range.fieldId, range.spec)
  val scanner = makeScanner(range, shard)
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
      sender ! findPosting(id, shard)
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

  /**
   *
   */
  def makeScanner(range: Range, shard: Int) = {
    val spec = range.spec
    range.fieldId.fieldType match {
      case DataType.TEXT =>
        val columnRange = makeColumnRange(Serializers.Text,
          spec.left.map(_.text.get.asInstanceOf[Object]), spec.right.map(_.text.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.text.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case DataType.LITERAL =>
        val columnRange = makeColumnRange(Serializers.Literal,
          spec.left.map(_.literal.get.asInstanceOf[Object]), spec.right.map(_.literal.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.literal.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case DataType.INTEGER =>
        val columnRange = makeColumnRange(Serializers.Integer,
          spec.left.map(_.integer.get.asInstanceOf[Object]), spec.right.map(_.integer.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.integer.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case DataType.FLOAT =>
        val columnRange = makeColumnRange(Serializers.Float,
          spec.left.map(_.float.get.asInstanceOf[Object]), spec.right.map(_.float.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.float.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case DataType.DATETIME =>
        val columnRange = makeColumnRange(Serializers.Datetime,
          spec.left.map(_.datetime.get.asInstanceOf[Object]), spec.right.map(_.datetime.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.datetime.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case DataType.ADDRESS =>
        val columnRange = makeColumnRange(Serializers.Address,
          spec.left.map(_.address.get.asInstanceOf[Object]), spec.right.map(_.address.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.address.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case DataType.HOSTNAME =>
        val columnRange = makeColumnRange(Serializers.Hostname,
          spec.left.map(_.hostname.get.asInstanceOf[Object]), spec.right.map(_.hostname.get.asInstanceOf[Object]),
          spec.leftExcl, spec.rightExcl)
        range.keyspace.prepareQuery(range.field.hostname.get.terms)
          .getKey(shard)
          .withColumnRange(columnRange)
          .autoPaginate(true)
      case unknown => throw new Exception("can't make Range scanner for " + range.toString)
    }
  }

  def makeColumnRange[P](ser: AnnotatedCompositeSerializer[P], left: Option[Object], right: Option[Object], leftExcl: Boolean, rightExcl: Boolean): ByteBufferRange = {
    val buildRange = ser.buildRange()
    (left,right) match {
      case (Some(_left), Some(_right)) =>
        val columnRange = if (leftExcl) buildRange.greaterThan(_left) else buildRange.greaterThanEquals(_left)
        if (rightExcl) columnRange.lessThan(_right) else columnRange.lessThanEquals(_right)
      case (None, Some(_right)) =>
        if (rightExcl) buildRange.lessThan(_right) else buildRange.lessThanEquals(_right)
      case (Some(_left), None) =>
        if (leftExcl) buildRange.greaterThan(_left) else buildRange.greaterThanEquals(_left)
      case _ => throw new Exception("RangeSpec is open on both ends")
    }
  }
  
  /**
   *
   */
  def findPosting(id: UUID, shard: Int): MatchResult = {
    val isWithinRange = range.fieldId.fieldType match {
      case DataType.TEXT =>
        val columnList = range.keyspace.prepareQuery(range.field.text.get.postings).getKey(id).execute().getResult
        val term = Text(columnList.getColumnByIndex(0).getStringValue)
        comparator.withinRange(term)
      case DataType.LITERAL =>
        val columnList = range.keyspace.prepareQuery(range.field.literal.get.postings).getKey(id).execute().getResult
        val term = Literal(columnList.getColumnByIndex(0).getStringValue)
        comparator.withinRange(term)
      case DataType.INTEGER =>
        val columnList = range.keyspace.prepareQuery(range.field.integer.get.postings).getKey(id).execute().getResult
        val term = Integer(columnList.getColumnByIndex(0).getLongValue)
        comparator.withinRange(term)
      case DataType.FLOAT =>
        val columnList = range.keyspace.prepareQuery(range.field.float.get.postings).getKey(id).execute().getResult
        val term = Float(columnList.getColumnByIndex(0).getDoubleValue)
        comparator.withinRange(term)
      case DataType.DATETIME =>
        val columnList = range.keyspace.prepareQuery(range.field.datetime.get.postings).getKey(id).execute().getResult
        val term = Datetime(columnList.getColumnByIndex(0).getDateValue)
        comparator.withinRange(term)
      case DataType.ADDRESS =>
        val columnList = range.keyspace.prepareQuery(range.field.address.get.postings).getKey(id).execute().getResult
        val term = Address(columnList.getColumnByIndex(0).getByteArrayValue)
        comparator.withinRange(term)
      case DataType.HOSTNAME =>
        val columnList = range.keyspace.prepareQuery(range.field.hostname.get.postings).getKey(id).execute().getResult
        val term = Hostname(columnList.getColumnByIndex(0).getStringValue)
        comparator.withinRange(term)
      case unknown => throw new Exception("can't make Range finder for " + range.toString)
    }
    if (isWithinRange) Right(BierPosting(id, PostingMetadata(None))) else Left(NoMoreMatches)
  }

}

/**
 *
 */
class RangeComparator(fieldId: FieldIdentifier, spec: RangeSpec) {

  def withinRange(value: DataValue): Boolean = value match {
    case text: Text =>
      withinRange(spec.left.map(m => compare(m, text)), spec.right.map(m => compare(m, text)), spec.leftExcl, spec.rightExcl)
    case literal: Literal =>
      withinRange(spec.left.map(m => compare(m, literal)), spec.right.map(m => compare(m, literal)), spec.leftExcl, spec.rightExcl)
    case integer: Integer =>
      withinRange(spec.left.map(m => compare(m, integer)), spec.right.map(m => compare(m, integer)), spec.leftExcl, spec.rightExcl)
    case float: Float =>
      withinRange(spec.left.map(m => compare(m, float)), spec.right.map(m => compare(m, float)), spec.leftExcl, spec.rightExcl)
    case datetime: Datetime =>
      withinRange(spec.left.map(m => compare(m, datetime)), spec.right.map(m => compare(m, datetime)), spec.leftExcl, spec.rightExcl)
    case address: Address =>
      withinRange(spec.left.map(m => compare(m, address)), spec.right.map(m => compare(m, address)), spec.leftExcl, spec.rightExcl)
    case hostname: Hostname =>
      withinRange(spec.left.map(m => compare(m, hostname)), spec.right.map(m => compare(m, hostname)), spec.leftExcl, spec.rightExcl)
  }

  def withinRange(start: Option[Int], end: Option[Int], startExcl: Boolean, endExcl: Boolean): Boolean = {
    start match {
      case Some(_start) if _start < 0 || startExcl && _start == 0 => return false
      case _ =>
    }
    end match {
      case Some(_end) if _end > 0 || endExcl && _end == 0 => return false
      case _ =>
    }
    true
  }

  def compare(matchTerm: MatchTerm, value: DataValue): Int = value match {
    case v: Text => v.compareTo(Text(matchTerm.text.get))
    case v: Literal => v.compareTo(Literal(matchTerm.literal.get))
    case v: Integer => v.compareTo(Integer(matchTerm.integer.get))
    case v: Float => v.compareTo(Float(matchTerm.float.get))
    case v: Datetime => v.compareTo(Datetime(matchTerm.datetime.get))
    case v: Address => v.compareTo(Address(matchTerm.address.get))
    case v: Hostname => v.compareTo(Hostname(matchTerm.hostname.get))
  }
}
