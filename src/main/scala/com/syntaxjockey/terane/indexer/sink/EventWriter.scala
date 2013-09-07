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

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import akka.agent.Agent
import com.netflix.astyanax.{Keyspace, MutationBatch}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.metadata.Store
import com.syntaxjockey.terane.indexer.cassandra.CassandraRowOperations
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics

class EventWriter(store: Store, val keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef, statsManager: ActorRef) extends Actor with ActorLogging with CassandraRowOperations {
  import EventWriter._
  import CassandraSink._
  import FieldManager._
  import StatsManager._

  var fieldsById: Map[FieldIdentifier,CassandraField] = Map.empty
  sinkBus.subscribe(self, classOf[FieldNotification])
  fieldManager ! GetFields

  var statsByCf: Map[String,Agent[FieldStatistics]] = Map.empty
  sinkBus.subscribe(self, classOf[StatsNotification])
  statsManager ! GetStats

  def receive = {

    /* attempt to store the incoming event */
    case StoreEvent(event, attempt) =>
      buildMutation(event, keyspace) match {
        case Left(missingFields) =>
          missingFields foreach { fieldManager ! _ }
          context.parent ! RetryEvent(event, attempt + 1)
        case Right(mutation) =>
          writeEvent(mutation) match {
            case Retry =>
              context.parent ! RetryEvent(event, attempt + 1)
            case Success =>
              context.parent ! WroteEvent(event)
            case Failure =>
              context.parent ! WriteFailed(event)
          }
      }

    /* update our cache of fields */
    case FieldMap(_fieldsById, _) =>
      fieldsById = _fieldsById

    /* update our cache of statistics */
    case StatsMap(_statsByCf) =>
      statsByCf = _statsByCf
  }


  /**
   * Returns Left(missingFields) if not all needed fields exist, otherwise Right(mutation).
   *
   * @param event
   * @param keyspace
   * @return
   */
   def buildMutation(event: BierEvent, keyspace: Keyspace): Either[Seq[CreateField],Mutation] = {
    log.debug("received event {}", event.id)
    /* create our batches */
    val postingsMutation = keyspace.prepareMutationBatch()
    val eventMutation = keyspace.prepareMutationBatch()
    /* build the event */
    val row = eventMutation.withRow(CassandraSink.CF_EVENTS, event.id)
    /* build the postings */
    var statistics: Map[String,FieldStatistics] = Map.empty
    var missingFields = Seq.empty[CreateField]
    for ((ident,value) <- event.values) {
      for (text <- value.text) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.text.get.id
            val stats = writeTextPosting(postingsMutation, field.text.get, text, event.id)
            row.putColumn(id, text.underlying)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (literal <- value.literal) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.literal.get.id
            val stats = writeLiteralPosting(postingsMutation, field.literal.get, literal, event.id)
            row.putColumn(id, literal.underlying)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (integer <- value.integer) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.integer.get.id
            val stats = writeIntegerPosting(postingsMutation, field.integer.get, integer, event.id)
            row.putColumn(id, integer.underlying)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (float <- value.float) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.float.get.id
            val stats = writeFloatPosting(postingsMutation, field.float.get, float, event.id)
            row.putColumn(id, float.underlying)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (datetime <- value.datetime) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.datetime.get.id
            val stats = writeDatetimePosting(postingsMutation, field.datetime.get, datetime, event.id)
            row.putColumn(id, datetime.underlying.toDate)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (address <- value.address) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.address.get.id
            val stats = writeAddressPosting(postingsMutation, field.address.get, address, event.id)
            row.putColumn(id, address.underlying.getAddress)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (hostname <- value.hostname) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            val id = field.hostname.get.id
            val stats = writeHostnamePosting(postingsMutation, field.hostname.get, hostname, event.id)
            row.putColumn(id, hostname.underlying.toString)
            statistics = statistics + (id -> stats)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
    }
    if (!missingFields.isEmpty) Left(missingFields) else Right(Mutation(event.id, eventMutation, postingsMutation, statistics))
  }

  /**
   * Returns Retry if the write failed and we should retry, Success if the write succeeded, or
   * Failure if the write failed in a way which we should not retry.
   *
   * @param mutation
   * @return
   */
  def writeEvent(mutation: Mutation): Result = {
    try {
      val result = mutation.event.execute()
      val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      log.debug("wrote event {} in {}", mutation.id, latency)
    } catch {
      case ex: Exception =>
        log.error(ex, "failed to write event {}", mutation.id)
    }
    /* execute the postings mutations */
    try {
      val result = mutation.postings.execute()
      val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      log.debug("wrote postings for {} in {}", mutation.id, latency)
      Success
    } catch {
      case ex: Exception =>
        log.error(ex, "failed to write postings for {}", mutation.id)
        Failure
    }
  }
}

object EventWriter {

  def props(store: Store, keyspace: Keyspace, sinkBus: SinkBus, fieldManager: ActorRef, statsManager: ActorRef) = {
    Props(classOf[EventWriter], store, keyspace, sinkBus, fieldManager, statsManager)
  }

  sealed trait Result
  case object Success extends Result
  case object Failure extends Result
  case object Retry extends Result
}

case class Mutation(id: UUID, event: MutationBatch, postings: MutationBatch, stats: Map[String,FieldStatistics])
