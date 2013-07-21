package com.syntaxjockey.terane.indexer.bier.matchers

import scala.concurrent.Future
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting, NoMoreMatches}
import com.syntaxjockey.terane.indexer.bier.{EventValueType, Matchers}
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import java.util.UUID

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

  def nextPosting = Future.failed(new NotImplementedError("TermMatcher doesn't implement nextPosting"))

  def findPosting(id: UUID) = Future.failed(new NotImplementedError("TermMatcher doesn't implement findPosting"))

  def close() {}
}

object TermMatcher {
  case class FieldIdentifier(fieldName: String, fieldType: EventValueType.Value)
}
