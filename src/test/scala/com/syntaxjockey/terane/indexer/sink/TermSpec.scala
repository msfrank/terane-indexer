package com.syntaxjockey.terane.indexer.sink

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.netflix.astyanax.Keyspace
import scala.concurrent.duration._
import java.util.UUID

import com.syntaxjockey.terane.indexer.{TestCluster, UUIDLike}
import com.syntaxjockey.terane.indexer.bier.Matchers.{Posting => BierPosting, NoMoreMatches}
import com.netflix.astyanax.util.TimeUUIDUtils
import scala.concurrent.Await
import org.joda.time.DateTime
import java.net.InetAddress
import org.xbill.DNS.Name

class TermSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with TestCluster {
  import TestCluster._

  // magic
  def this() = this(ActorSystem("TermSpec"))

  def withKeyspace(runTest: Keyspace => Any) {
    val client = getCassandraClient
    val id = "test_" + new UUIDLike(UUID.randomUUID()).toString
    val keyspace = createKeyspace(client, id)
    try {
      runTest(keyspace)
    } finally {
      keyspace.dropKeyspace().getResult
      client.close()
    }
  }

  "A Term" must {

    "return a UUID for a Text value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, textField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      keyspace.writeTextPosting(mutation, textCf, "foo", id)
      mutation.execute().getResult
      val term = Term(textId, "foo", keyspace, textField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for a Literal value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, literalField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      keyspace.writeLiteralPosting(mutation, literalCf, List("foo"), id)
      mutation.execute().getResult
      val term = Term(literalId, "foo", keyspace, literalField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Integer value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, integerField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      keyspace.writeIntegerPosting(mutation, integerCf, 42L, id)
      mutation.execute().getResult
      val term = Term(integerId, 42L, keyspace, integerField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for a Float value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, floatField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      keyspace.writeFloatPosting(mutation, floatCf, 3.14159, id)
      mutation.execute().getResult
      val term = Term(floatId, 3.14159, keyspace, floatField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Datetime value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, datetimeField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val now = DateTime.now()
      keyspace.writeDatetimePosting(mutation, datetimeCf, now, id)
      mutation.execute().getResult
      val term = Term(datetimeId, now.toDate, keyspace, datetimeField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Address value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, addressField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val addr = InetAddress.getLocalHost
      keyspace.writeAddressPosting(mutation, addressCf, addr, id)
      mutation.execute().getResult
      val term = Term(addressId, addr.getAddress, keyspace, addressField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }

    "return a UUID for an Hostname value" in withKeyspace { keyspace =>
      createColumnFamily(keyspace, hostnameField)
      val mutation = keyspace.prepareMutationBatch()
      val id = TimeUUIDUtils.getUniqueTimeUUIDinMicros
      val host = Name.fromString("com")
      keyspace.writeHostnamePosting(mutation, hostnameCf, host, id)
      mutation.execute().getResult
      val term = Term(hostnameId, "com", keyspace, hostnameField)
      inside(Await.result(term.nextPosting, 10 seconds)) {
        case Right(BierPosting(postingId, postingMetdata)) =>
          postingId must be(id)
      }
      Await.result(term.nextPosting, 10 seconds) must be(Left(NoMoreMatches))
    }
  }
}