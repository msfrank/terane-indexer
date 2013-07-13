package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.Matchers._
import java.util.UUID
import scala.concurrent.Future

/**
 * All matchers must derive from this abstract class.
 */
abstract class Matchers {
  def getNextPosting: Future[Either[NoMoreMatches.type,Posting]]
}

object Matchers {
  case class Posting(id: UUID, postingMetadata: PostingMetadata)
  case object NoMoreMatches
}

