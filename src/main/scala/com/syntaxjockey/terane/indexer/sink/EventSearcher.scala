package com.syntaxjockey.terane.indexer.sink

import com.syntaxjockey.terane.indexer.bier.{Event, Matchers, Searcher}
import com.netflix.astyanax.{Keyspace, Cluster}
import akka.event.LoggingAdapter
import java.util.UUID
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher

trait EventSearcher extends Searcher with FieldManager with EventReader {

  def log: LoggingAdapter
  def csKeyspace: Keyspace
  def csCluster: Cluster

  /**
   * Convert an unbound TermMatcher into a Term bound to the cassandra sink.
   *
   * @param matcher
   * @tparam T
   * @return
   */
  def optimizeTermMatcher[T](matcher: TermMatcher[T]): Matchers = {
    new Term(matcher.field, matcher.term, csKeyspace, this)
  }

  /**
   * Given a list of event ids, retrieve each event associated with the ids.
   *
   * @param postings
   * @return
   */
  def getEvents(postings: List[UUID]): List[Event] = {
    log.debug("looking up events {}", postings)
    val result = csKeyspace.prepareQuery(CassandraSink.CF_EVENTS).getKeySlice(postings).execute()
    val latency = Duration(result.getLatency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    log.debug("getEvents took {}", latency)
    for (row <- result.getResult.toList) yield readEvent(row.getKey, row.getColumns)
  }
}
