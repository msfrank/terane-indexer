package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher

trait Searcher {

  def optimizeTermMatcher[T](term: TermMatcher[T]): Matchers
}
