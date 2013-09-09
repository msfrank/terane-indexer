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

import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.bier.FieldIdentifier
import com.syntaxjockey.terane.indexer.bier.Matchers

/**
 * Match the term of the specified type in the specified field.  This class is a
 * placeholder for the backend-specific term matcher, which has more information about
 * the actual term storage, and thus can make better decisions about how to implement
 * the interface methods.
 *
 * @param fieldId
 * @param term
 * @tparam T
 */
case class TermMatcher[T](fieldId: FieldIdentifier, term: T) extends Matchers {
  def estimateCost = 0L
  def nextPosting = Future.failed(new NotImplementedError("TermMatcher doesn't implement nextPosting"))
  def findPosting(id: UUID) = Future.failed(new NotImplementedError("TermMatcher doesn't implement findPosting"))
  def close() {}
  def hashString: String = "%s:%s[%s]=\"%s\"".format(this.getClass.getName, fieldId.fieldName, fieldId.fieldType.toString.toLowerCase, term.toString)
}
