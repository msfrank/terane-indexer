package com.syntaxjockey.terane.indexer.bier.matchers

import com.syntaxjockey.terane.indexer.bier.{Searcher, Matchers}
import scala.concurrent.Future
import com.syntaxjockey.terane.indexer.bier.Matchers.NoMoreMatches
import java.util.UUID

/**
 * Matches terms which are the intersection of all child matchers.
 *
 * @param children
 */
case class AndMatcher(children: List[Matchers]) extends Matchers {

  def nextPosting = Future.successful(Left(NoMoreMatches))

  def findPosting(id: UUID) = Future.successful(Left(NoMoreMatches))

  def close() {
    children.foreach(_.close())
  }
}