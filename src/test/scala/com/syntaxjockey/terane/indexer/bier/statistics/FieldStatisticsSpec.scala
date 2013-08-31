package com.syntaxjockey.terane.indexer.bier.statistics

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class FieldStatisticsSpec extends WordSpec with MustMatchers {

  "A FieldStatistics instance" must {

    "process a sequence of String objects" in {
      val strings: Seq[Analytical] = Seq("hello", "hello", "hello", "world")
      val stat = FieldStatistics(strings)
      stat.estimateFieldCardinality.estimate must be === 2
      stat.estimateTermFrequency("hello").estimate must be === 3
      stat.estimateTermSetContains("hello").isTrue must be === true
      stat.estimateTermSetContains("world").isTrue must be === true
      stat.estimateTermSetContains("foo").isTrue must be === false
      stat.getTermCount must be === 4
    }

  }
}
