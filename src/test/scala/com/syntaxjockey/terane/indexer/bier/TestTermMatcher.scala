package com.syntaxjockey.terane.indexer.bier

import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers.{NoMoreMatches, Posting, MatchResult}

case class TestTermMatcher(postings: List[Posting]) extends Matchers {
  import scala.concurrent.ExecutionContext.Implicits.global

  var postingsLeft = postings

  def nextPosting = Future[MatchResult] {
    if (postingsLeft.isEmpty) {
      Left(NoMoreMatches)
    } else {
      val posting = postingsLeft.head
      postingsLeft = postingsLeft.tail
      Right(posting)
    }
  }

  def findPosting(id: UUID) = Future[MatchResult] {
    postings.find(_.id == id) match {
      case Some(posting) =>
        Right(posting)
      case None =>
        Left(NoMoreMatches)
    }
  }

  def close() { }
}
