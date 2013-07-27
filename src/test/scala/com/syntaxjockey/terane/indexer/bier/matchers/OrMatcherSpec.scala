package com.syntaxjockey.terane.indexer.bier.matchers

import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.TestTermMatcher
import com.syntaxjockey.terane.indexer.bier.Matchers.{MatchResult, NoMoreMatches, Posting}
import com.syntaxjockey.terane.indexer.bier.Field.PostingMetadata

class OrMatcherSpec(_system: ActorSystem) extends TestKit(_system) with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("OrMatcherSpec"))

  override def afterAll() {
    system.shutdown()
  }

  val ids = Seq(
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    UUID.randomUUID()
  ).sortWith { case (left: UUID, right: UUID) =>
      if (left.toString < right.toString) true else false
  }

  "An OrMatcher" must {

    "return the union" in {
      val orMatcher = OrMatcher(List(
        TestTermMatcher(List(
          Posting(ids(0), PostingMetadata(None)),
          Posting(ids(2), PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(ids(1), PostingMetadata(None)),
          Posting(ids(4), PostingMetadata(None))
        ))
      ))
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(0))
      }
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(1))
      }
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(2))
      }
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(4))
      }
      Await.result(orMatcher.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "find a Posting if it is present in any child matchers" in {
      val orMatcher = OrMatcher(List(
        TestTermMatcher(List(
          Posting(ids(0), PostingMetadata(None)),
          Posting(ids(2), PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(ids(1), PostingMetadata(None)),
          Posting(ids(2), PostingMetadata(None))
        ))
      ))
      inside(Await.result(orMatcher.findPosting(ids(0)), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(0))
      }
      inside(Await.result(orMatcher.findPosting(ids(1)), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(1))
      }
      inside(Await.result(orMatcher.findPosting(ids(2)), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(ids(2))
      }
    }

    "not find a Posting if it is not present in any child matchers" in {
      val orMatcher = OrMatcher(List(
        TestTermMatcher(List(
          Posting(ids(0), PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(ids(1), PostingMetadata(None))
        ))
      ))
      Await.result(orMatcher.findPosting(ids(2)), 10 seconds) must be(Left(NoMoreMatches))
    }
  }
}