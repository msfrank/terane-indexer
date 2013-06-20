package com.syntaxjockey.terane.indexer.bier.matchers

import com.syntaxjockey.terane.indexer.bier.{Searcher, Matchers}
import scala.concurrent.Future
import com.syntaxjockey.terane.indexer.bier.Matchers.NoMoreMatches

/**
 * Matches terms which are the intersection of all child matchers.
 *
 * @param children
 */
class AndMatcher(val children: List[Matchers]) extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = this
  def getNextPosting = Future.successful(Left(NoMoreMatches))
}