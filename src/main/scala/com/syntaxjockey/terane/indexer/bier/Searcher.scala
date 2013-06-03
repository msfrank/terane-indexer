package com.syntaxjockey.terane.indexer.bier

trait Searcher {

  def optimizeTermMatcher[T](term: TermMatcher[T]): Matchers

}
