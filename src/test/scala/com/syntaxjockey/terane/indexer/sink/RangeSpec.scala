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

package com.syntaxjockey.terane.indexer.sink

import scala.language.postfixOps

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._
import akka.agent.Agent
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.util.TimeUUIDUtils
import scala.concurrent.duration._
import scala.concurrent.Await
import java.util.UUID

import com.syntaxjockey.terane.indexer.{RequiresTestCluster, TestCluster, UUIDLike}
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, NoMoreMatches}
import com.syntaxjockey.terane.indexer.bier.MatchTerm
import com.syntaxjockey.terane.indexer.bier.statistics.FieldStatistics
import com.syntaxjockey.terane.indexer.bier.matchers.{RightOpenRangeSpec, LeftOpenRangeSpec, ClosedRangeSpec}
import com.syntaxjockey.terane.indexer.bier.matchers.{RangeSpec => _RangeSpec}

class RangeSpec extends TestCluster("RangeSpec") with WordSpec with MustMatchers {
  import scala.concurrent.ExecutionContext.Implicits.global
  import TestCluster._

  def withKeyspace(runTest: Keyspace => Any) {
    val client = getCassandraClient
    val id = "test_" + new UUIDLike(UUID.randomUUID()).toString
    val keyspace = createKeyspace(client, id)
    try {
      runTest(keyspace)
    } finally {
      keyspace.dropKeyspace().getResult
    }
  }

  "A Range for a Literal field" must {

    val id01 = TimeUUIDUtils.getUniqueTimeUUIDinMicros
    val id02 = TimeUUIDUtils.getUniqueTimeUUIDinMicros
    val id03 = TimeUUIDUtils.getUniqueTimeUUIDinMicros

    def populateFieldAndGetMatcher(keyspace: Keyspace, spec: _RangeSpec): Range = {
      createColumnFamily(keyspace, literalField)
      val mutation = keyspace.prepareMutationBatch()
      val stats01 = keyspace.writeLiteralPosting(mutation, literalCf, Literal("a"), id01)
      val stats02 = keyspace.writeLiteralPosting(mutation, literalCf, Literal("b"), id02)
      val stats03 = keyspace.writeLiteralPosting(mutation, literalCf, Literal("c"), id03)
      mutation.execute().getResult
      val stats = FieldStatistics.merge(Seq(stats01, stats02, stats03))
      Range(literalId, spec, keyspace, literalField, Some(Agent(stats)))
    }

    "return postings for a closed range which is left- and right-inclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = ClosedRangeSpec(MatchTerm(literal = Some("a")), MatchTerm(literal = Some("c")), startExcl = false, endExcl = false)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id01)
      }
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id02)
      }
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id03)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return postings for a closed range which is left- and right-exclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = ClosedRangeSpec(MatchTerm(literal = Some("a")), MatchTerm(literal = Some("c")), startExcl = true, endExcl = true)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id02)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }


    "return postings for a closed range which is left-inclusive and right-exclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = ClosedRangeSpec(MatchTerm(literal = Some("a")), MatchTerm(literal = Some("c")), startExcl = false, endExcl = true)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id01)
      }
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id02)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }


    "return postings for a closed range which is left-exclusive and right-inclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = ClosedRangeSpec(MatchTerm(literal = Some("a")), MatchTerm(literal = Some("c")), startExcl = true, endExcl = false)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id02)
      }
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id03)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }


    "return postings for a left-open range which is right-inclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = LeftOpenRangeSpec(MatchTerm(literal = Some("b")), endExcl = false)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id01)
      }
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id02)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return postings for a left-open range which is right-exclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = LeftOpenRangeSpec(MatchTerm(literal = Some("b")), endExcl = true)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id01)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return postings for a right-open range which is left-inclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = RightOpenRangeSpec(MatchTerm(literal = Some("b")), startExcl = false)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id02)
      }
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id03)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return postings for a right-open range which is left-exclusive" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      val spec = RightOpenRangeSpec(MatchTerm(literal = Some("b")), startExcl = true)
      val range = populateFieldAndGetMatcher(keyspace, spec)
      inside(Await.result(range.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id03)
      }
      Await.result(range.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

  }
}