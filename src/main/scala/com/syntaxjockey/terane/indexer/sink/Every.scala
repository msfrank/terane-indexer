package com.syntaxjockey.terane.indexer.sink

import com.netflix.astyanax.Keyspace
import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.Matchers

case class Every(keyspace: Keyspace) extends Matchers {
  import Matchers._

  def estimateCost: Long = 0L
  def nextPosting = Future.successful(Left(NoMoreMatches))
  def findPosting(id: UUID) = Future.successful(Left(NoMoreMatches))
  def close() { }
}
