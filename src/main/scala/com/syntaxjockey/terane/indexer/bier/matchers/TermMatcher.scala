package com.syntaxjockey.terane.indexer.bier.matchers

import scala.concurrent.Future
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting, NoMoreMatches}
import com.syntaxjockey.terane.indexer.bier.{EventValueType, Searcher, Matchers}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier

/**
 * Match the term of the specified type in the specified field.  This class is a
 * placeholder for the backend-specific term matcher, which has more information about
 * the actual term storage, and thus can make better decisions about how to implement
 * the interface methods.
 *
 * @param fieldId
 * @param term
 * @tparam T
 */
case class TermMatcher[T](fieldId: FieldIdentifier, term: T) extends Matchers {

  def optimizeMatcher(searcher: Searcher): Matchers = searcher.optimizeTermMatcher[T](this)
  def getNextPosting: Future[Either[NoMoreMatches.type,Posting]] = Future.successful(Left(NoMoreMatches))
}

object TermMatcher {
  case class FieldIdentifier(fieldName: String, fieldType: EventValueType.Value)
}
