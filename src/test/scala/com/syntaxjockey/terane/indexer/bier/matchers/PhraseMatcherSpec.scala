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

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.TestTermMatcher
import com.syntaxjockey.terane.indexer.bier.Matchers.{NoMoreMatches, Posting}
import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata
import com.syntaxjockey.terane.indexer.TestCluster

class PhraseMatcherSpec extends TestCluster("PhraseMatcherSpec") with WordSpec with MustMatchers {

  val id01 = UUID.randomUUID()
  val id02 = UUID.randomUUID()
  val id03 = UUID.randomUUID()
  val id04 = UUID.randomUUID()
  val id05 = UUID.randomUUID()
  val id06 = UUID.randomUUID()
  val id07 = UUID.randomUUID()
  val id08 = UUID.randomUUID()

  "A PhraseMatcher" must {

    "match a two term phrase" in {
      val phraseMatcher = PhraseMatcher(Seq(
        TestTermMatcher(List(
          Posting(id01, PostingMetadata(Some(scala.collection.mutable.Set(1)))),
          Posting(id02, PostingMetadata(Some(scala.collection.mutable.Set(1)))),
          Posting(id03, PostingMetadata(Some(scala.collection.mutable.Set(1)))),
          Posting(id04, PostingMetadata(Some(scala.collection.mutable.Set(1)))),
          Posting(id06, PostingMetadata(Some(scala.collection.mutable.Set(5))))
        )),
        TestTermMatcher(List(
          Posting(id02, PostingMetadata(Some(scala.collection.mutable.Set(2)))),
          Posting(id03, PostingMetadata(Some(scala.collection.mutable.Set(3)))),
          Posting(id05, PostingMetadata(Some(scala.collection.mutable.Set(2)))),
          Posting(id06, PostingMetadata(Some(scala.collection.mutable.Set(6)))),
          Posting(id07, PostingMetadata(Some(scala.collection.mutable.Set(2)))),
          Posting(id08, PostingMetadata(Some(scala.collection.mutable.Set(5))))
        ))
      ))
      inside(Await.result(phraseMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id02)
      }
      inside(Await.result(phraseMatcher.nextPosting, 10 seconds)) {
        case Right(Posting(id, _)) =>
          id must be(id06)
      }
      Await.result(phraseMatcher.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

  }
}
