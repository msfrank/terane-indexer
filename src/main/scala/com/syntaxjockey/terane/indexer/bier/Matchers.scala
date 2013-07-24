package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.Matchers._
import java.util.UUID
import scala.concurrent.Future

/**
 * All matchers must derive from this abstract class.
 */
abstract class Matchers {
  import Matchers._
  def nextPosting: Future[MatchResult]
  def findPosting(id: UUID): Future[MatchResult]
  def close()
}

object Matchers {
  type MatchResult = Either[NoMoreMatches.type,Posting]
  case object NoMoreMatches
  case class Posting(id: UUID, postingMetadata: PostingMetadata)
  case object NextPosting
  case class FindPosting(id: UUID)
}

