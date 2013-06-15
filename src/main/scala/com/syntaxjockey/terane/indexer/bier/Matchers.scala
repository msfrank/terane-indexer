package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import java.util.UUID
import com.syntaxjockey.terane.indexer.bier.Matchers._
import scala.concurrent.{ExecutionContext, Future}

trait Matchers {
  def optimizeMatcher(searcher: Searcher): Matchers
  def getNextPosting: Future[Either[NoMoreMatches.type,Posting]]
}

object Matchers {
  case class Posting(id: UUID, postingMetadata: PostingMetadata)
  case object NoMoreMatches
}

/**
 * Match the term of the specified type in the specified field.
 *
 * @param name
 * @param term
 * @tparam T
 */
class TermMatcher[T](val name: String, val term: T) extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = searcher.optimizeTermMatcher[T](this)
  def getNextPosting: Future[Either[NoMoreMatches.type,Posting]] = Future.successful(Left(NoMoreMatches))
}

/*
class Or extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = this
  def getNextPosting = Future.successful(Left(NoMoreMatches))
}

class And extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = this
  def getNextPosting = Future.successful(Left(NoMoreMatches))
}
*/
