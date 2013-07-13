package com.syntaxjockey.terane.indexer.bier.matchers

import com.syntaxjockey.terane.indexer.bier.{Searcher, Matchers}
import scala.concurrent.Future
import com.syntaxjockey.terane.indexer.bier.Matchers.NoMoreMatches


/**
 * Matches terms which are the union of all child matchers.
 *
 * @param children
 */
case class OrMatcher(children: List[Matchers]) extends Matchers {

  def getNextPosting = Future.successful(Left(NoMoreMatches))
}