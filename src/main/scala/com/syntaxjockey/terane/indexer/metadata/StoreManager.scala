package com.syntaxjockey.terane.indexer.metadata

import java.util.UUID
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import org.joda.time.DateTime
import com.syntaxjockey.terane.indexer.bier.EventValueType

trait StoreManager {
  import StoreManager._

  private val storesById = scala.collection.mutable.HashMap[UUID,Store]()
  private val storesByName = scala.collection.mutable.HashMap[String,Store]()

  def getStore(id: UUID): Option[Store] = storesById.get(id)

  def getStore(name: String): Option[Store] = storesByName.get(name)

  def putStore(store: Store) {
    storesById.put(store.id, store)
    storesByName.put(store.storeName, store)
  }

  def removeStore(store: Store) {
    storesById.remove(store.id)
    storesByName.remove(store.storeName)
  }
}

object StoreManager {
  case class Store(id: UUID, storeName: String, created: DateTime, fieldsById: Map[UUID,Field], fieldsByIdentifer: Map[FieldIdentifier,Field])
  case class Field(id: UUID, fieldName: String, fieldType: EventValueType.Value, created: DateTime)
}
