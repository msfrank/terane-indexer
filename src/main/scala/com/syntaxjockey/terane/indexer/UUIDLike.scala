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
