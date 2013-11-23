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
  def hashString = this.getClass.getName + ":"
}
