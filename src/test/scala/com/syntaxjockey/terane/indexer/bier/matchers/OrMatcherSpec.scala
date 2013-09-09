/**
 * Copyright 2013 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Terane.
 *
 * Terane is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Terane is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.syntaxjockey.terane.indexer.bier.matchers

import scala.language.postfixOps

import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.{Matchers, TestTermMatcher}
import com.syntaxjockey.terane.indexer.bier.Matchers.{MatchResult, NoMoreMatches, Posting}
import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata

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
      val orMatcher = OrMatcher(Set(
        TestTermMatcher(List(
          Posting(ids(0), PostingMetadata(None)),
          Posting(ids(2), PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(ids(1), PostingMetadata(None)),
          Posting(ids(4), PostingMetadata(None))
        ))
      ).toSet[Matchers])
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
      val orMatcher = OrMatcher(Set(
        TestTermMatcher(List(
          Posting(ids(0), PostingMetadata(None)),
          Posting(ids(2), PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(ids(1), PostingMetadata(None)),
          Posting(ids(2), PostingMetadata(None))
        ))
      ).toSet[Matchers])
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
      val orMatcher = OrMatcher(Set(
        TestTermMatcher(List(
          Posting(ids(0), PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(ids(1), PostingMetadata(None))
        ))
      ).toSet[Matchers])
      Await.result(orMatcher.findPosting(ids(2)), 10 seconds) must be(Left(NoMoreMatches))
    }
  }
}