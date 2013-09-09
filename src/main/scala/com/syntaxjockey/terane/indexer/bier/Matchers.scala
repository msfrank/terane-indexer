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

package com.syntaxjockey.terane.indexer.bier

import com.syntaxjockey.terane.indexer.bier.BierField.PostingMetadata
import com.syntaxjockey.terane.indexer.bier.Matchers._
import java.util.UUID
import scala.concurrent.Future

/**
 * All matchers must derive from this abstract class.
 */
abstract class Matchers {
  import Matchers._

  def estimateCost: Long
  def nextPosting: Future[MatchResult]
  def findPosting(id: UUID): Future[MatchResult]
  def close()
  def hashString: String

  // override equals and hashCode to we can use Set operations on Matchers
  override def equals(other: Any): Boolean = other match {
    case matcher: Matchers =>
      this.hashString.equals(matcher.hashString)
    case _ =>
      false
  }
  override def hashCode(): Int = this.hashString.hashCode
}

object Matchers {
  type MatchResult = Either[NoMoreMatches.type,Posting]
  case object NoMoreMatches
  case class Posting(id: UUID, postingMetadata: PostingMetadata)
  case object NextPosting
  case class FindPosting(id: UUID)
}

