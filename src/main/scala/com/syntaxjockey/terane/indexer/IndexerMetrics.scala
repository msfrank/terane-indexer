package com.syntaxjockey.terane.indexer

import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.InstrumentedBuilder

/**
 * Container for metrics registry
 */
object IndexerMetrics {
  val metricRegistry = new MetricRegistry()
}

/**
 * Apply this trait to gain access to metrics
 */
trait Instrumented extends InstrumentedBuilder {
  val metricRegistry = IndexerMetrics.metricRegistry
}
