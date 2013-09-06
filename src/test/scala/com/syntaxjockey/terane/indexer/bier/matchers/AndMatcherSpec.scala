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
import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata

class AndMatcherSpec(_system: ActorSystem) extends TestKit(_system) with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AndMatcherSpec"))

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

  "An AndMatcher" must {

    "return the intersection" in {
      val andMatcher = AndMatcher(List(
        TestTermMatcher(List(
          Posting(id01, PostingMetadata(None)),
          Posting(id02, PostingMetadata(None)),
          Posting(id03, PostingMetadata(None)),
          Posting(id04, PostingMetadata(None)),
          Posting(id06, PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None)),
          Posting(id03, PostingMetadata(None)),
          Posting(id05, PostingMetadata(None)),
          Posting(id06, PostingMetadata(None)),
          Posting(id07, PostingMetadata(None))
        ))
      ))
      inside(Await.result(andMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id02)
      }
      inside(Await.result(andMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id03)
      }
      inside(Await.result(andMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id06)
      }
      Await.result(andMatcher.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "find a Posting if it is present in all child matchers" in {
      val andMatcher = AndMatcher(List(
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None))
        ))
      ))
      inside(Await.result(andMatcher.findPosting(id02), 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id02)
      }
    }

    "not find a Posting if it is not present in all child matchers" in {
      val andMatcher = AndMatcher(List(
        TestTermMatcher(List(
          Posting(id01, PostingMetadata(None))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(None))
        ))
      ))
      Await.result(andMatcher.findPosting(id01), 10 seconds) must be(Left(NoMoreMatches))
    }
  }
}
