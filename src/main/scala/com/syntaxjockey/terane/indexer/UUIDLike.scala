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

package com.syntaxjockey.terane.indexer

import java.util.UUID

class UUIDLike(private val uuid: UUID) {
  override def toString: String = uuid.toString.split('-').mkString.toLowerCase
}

object UUIDLike {
  def apply(uuid: UUID) = new UUIDLike(uuid)
  implicit def uuid2UUIDLike(uuid: UUID) = new UUIDLike(uuid)
  implicit def string2UUIDLike(str: String) = new UUIDLike(UUID.fromString(str))
  implicit def UUIDLike2String(uuidlike: UUIDLike): String = uuidlike.toString
}
