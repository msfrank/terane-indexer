package com.syntaxjockey.terane.indexer.cassandra

import com.netflix.astyanax.model.ColumnFamily

case class FieldColumnFamily(name: String, id: String, width: Long)

class TypedFieldColumnFamily[F,P](
  override val name: String,
  override val id: String,
  override val width: Long,
  val field: F,
  val cf: ColumnFamily[java.lang.Long,P])
extends FieldColumnFamily(name, id, width)
