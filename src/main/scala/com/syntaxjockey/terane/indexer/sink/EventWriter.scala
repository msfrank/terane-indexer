package com.syntaxjockey.terane.indexer.sink

import akka.actor.{ActorRef, Actor, ActorLogging}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.netflix.astyanax.{Keyspace, MutationBatch}
import java.util.UUID
import scala.collection.JavaConversions._

import com.syntaxjockey.terane.indexer.bier._
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.syntaxjockey.terane.indexer.metadata.StoreManager.Store
import com.syntaxjockey.terane.indexer.cassandra.CassandraRowOperations

class EventWriter(store: Store, val keyspace: Keyspace, fieldManager: ActorRef) extends Actor with ActorLogging with CassandraRowOperations {
  import CassandraSink._
  import EventWriter._
  import FieldManager._

  fieldManager ! GetFields

  var fieldsById: Map[FieldIdentifier,Field] = Map.empty

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
    case FieldsChanged(_fieldsById, _) =>
      fieldsById = _fieldsById
  }


  /**
   * Returns Left(missingFields) if not all needed fields exist, otherwise Right(mutation).
   *
   * @param event
   * @param keyspace
   * @return
   */
   def buildMutation(event: Event, keyspace: Keyspace): Either[Seq[CreateField],Mutation] = {
    log.debug("received event {}", event.id)
    /* create our batches */
    val postingsMutation = keyspace.prepareMutationBatch()
    val eventMutation = keyspace.prepareMutationBatch()
    /* build the event */
    val row = eventMutation.withRow(CassandraSink.CF_EVENTS, event.id)
    /* build the postings */
    var missingFields = Seq.empty[CreateField]
    for ((ident,value) <- event.values) {
      for (text <- value.text) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.text.get.id, text.underlying)
            writeTextPosting(postingsMutation, field.text.get, text, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (literal <- value.literal) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.literal.get.id, literal.underlying)
            writeLiteralPosting(postingsMutation, field.literal.get, literal, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (integer <- value.integer) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.integer.get.id, integer.underlying)
            writeIntegerPosting(postingsMutation, field.integer.get, integer, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (float <- value.float) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.float.get.id, float.underlying)
            writeFloatPosting(postingsMutation, field.float.get, float, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (datetime <- value.datetime) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.datetime.get.id, datetime.underlying.toDate)
            writeDatetimePosting(postingsMutation, field.datetime.get, datetime, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (address <- value.address) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.address.get.id, address.underlying.getAddress)
            writeAddressPosting(postingsMutation, field.address.get, address, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
      for (hostname <- value.hostname) {
        fieldsById.get(ident) match {
          case Some(field) if missingFields.isEmpty =>
            row.putColumn(field.hostname.get.id, hostname.underlying.toString)
            writeHostnamePosting(postingsMutation, field.hostname.get, hostname, event.id)
          case Some(field) => // do nothing
          case None =>
            missingFields = missingFields :+ CreateField(ident)
        }
      }
    }
    if (!missingFields.isEmpty) Left(missingFields) else Right(Mutation(event.id, eventMutation,postingsMutation))
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
  sealed trait Result
  case object Success extends Result
  case object Failure extends Result
  case object Retry extends Result

  case class Mutation(id: UUID, event: MutationBatch, postings: MutationBatch)
}
