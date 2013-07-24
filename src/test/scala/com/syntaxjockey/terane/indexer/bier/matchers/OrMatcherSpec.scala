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

  val id01 = UUID.randomUUID()
  val id02 = UUID.randomUUID()
  val id03 = UUID.randomUUID()
  val id04 = UUID.randomUUID()
  val id05 = UUID.randomUUID()
  val id06 = UUID.randomUUID()
  val id07 = UUID.randomUUID()

  "An OrMatcher" must {

    "return the union" in {
      val orMatcher = OrMatcher(List(
        TestTermMatcher(List(
          Posting(id01, PostingMetadata(None)),
          Posting(id03, PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None)),
          Posting(id05, PostingMetadata(None))
        ))
      ))
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id01)
      }
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id02)
      }
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id03)
      }
      inside(Await.result(orMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id05)
      }
      Await.result(orMatcher.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "find a Posting if it is present in any child matchers" in {
      val orMatcher = OrMatcher(List(
        TestTermMatcher(List(
          Posting(id01, PostingMetadata(None)),
          Posting(id03, PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None)),
          Posting(id03, PostingMetadata(None))
        ))
      ))
      inside(Await.result(orMatcher.findPosting(id01), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id01)
      }
      inside(Await.result(orMatcher.findPosting(id02), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id02)
      }
      inside(Await.result(orMatcher.findPosting(id03), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id03)
      }
    }

    "not find a Posting if it is not present in any child matchers" in {
      val orMatcher = OrMatcher(List(
        TestTermMatcher(List(
          Posting(id01, PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None))
        ))
      ))
      Await.result(orMatcher.findPosting(id03), 10 seconds) must be(Left(NoMoreMatches))
    }
  }
}