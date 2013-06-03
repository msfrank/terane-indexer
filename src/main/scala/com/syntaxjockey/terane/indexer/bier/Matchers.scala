package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import java.util.UUID

trait Matchers extends Iterable[(UUID,PostingMetadata)] {

  def optimizeMatcher(searcher: Searcher): Matchers

}

class TermMatcher[T](val name: String, val term: T) extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = {
    searcher.optimizeTermMatcher[T](this)
  }

  def iterator: Iterator[(UUID,PostingMetadata)] = {
    Seq.empty.iterator
  }

  override def size: Int = 0
}

class Or extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = {
    this
  }

  def iterator: Iterator[(UUID,PostingMetadata)] = {
    Seq.empty.iterator
  }

  override def size: Int = 0
}

class And extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = {
    this
  }

  def iterator: Iterator[(UUID,PostingMetadata)] = {
    Seq.empty.iterator
  }

  override def size: Int = 0
}
