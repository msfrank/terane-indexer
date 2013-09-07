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
import org.xbill.DNS.Name
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.Await
import java.net.InetAddress
import java.util.UUID

import com.syntaxjockey.terane.indexer.{RequiresTestCluster, TestCluster, UUIDLike}
import com.syntaxjockey.terane.indexer.bier.datatypes._
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, NoMoreMatches}

class TermSpec extends TestCluster("TermSpec") with WordSpec with MustMatchers {
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

  "A Term" must {

    "return a UUID for a Text value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, textField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val stats = keyspace.writeTextPosting(mutation, textCf, Text("foo"), id)
      mutation.execute().getResult
      val term = Term(textId, "foo", keyspace, textField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for a Literal value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, literalField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val stats = keyspace.writeLiteralPosting(mutation, literalCf, Literal("foo"), id)
      mutation.execute().getResult
      val term = Term(literalId, "foo", keyspace, literalField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Integer value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, integerField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val stats = keyspace.writeIntegerPosting(mutation, integerCf, Integer(42), id)
      mutation.execute().getResult
      val term = Term(integerId, 42L, keyspace, integerField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for a Float value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, floatField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val stats = keyspace.writeFloatPosting(mutation, floatCf, Float(3.14159), id)
      mutation.execute().getResult
      val term = Term(floatId, 3.14159, keyspace, floatField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Datetime value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, datetimeField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val now = DateTime.now()
      val stats = keyspace.writeDatetimePosting(mutation, datetimeCf, Datetime(now), id)
      mutation.execute().getResult
      val term = Term(datetimeId, now.toDate, keyspace, datetimeField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Address value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, addressField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val addr = InetAddress.getLocalHost
      val stats = keyspace.writeAddressPosting(mutation, addressCf, Address(addr), id)
      mutation.execute().getResult
      val term = Term(addressId, addr.getAddress, keyspace, addressField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Hostname value" taggedAs RequiresTestCluster in withKeyspace { keyspace =>
      createColumnFamily(keyspace, hostnameField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val host = Name.fromString("com")
      val stats = keyspace.writeHostnamePosting(mutation, hostnameCf, Hostname(host), id)
      mutation.execute().getResult
      val term = Term(hostnameId, "com", keyspace, hostnameField, Some(Agent(stats)))
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }
  }
}
