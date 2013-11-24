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

package com.syntaxjockey.terane.indexer.metadata

import akka.actor.{ActorRef, Props, Actor, ActorLogging}
import akka.pattern.pipe
import akka.actor.Status.Failure
import com.netflix.curator.framework.recipes.locks.InterProcessReadWriteLock
import org.apache.zookeeper.data.Stat
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.cassandra.Cassandra
import com.syntaxjockey.terane.indexer.{ResourceNotFound, UUIDLike}

/**
 * + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      - "created" -> Long
 *      - "count" -> UUID
 *      + "fields"
 */
class StoreManager extends Actor with ActorLogging {
  import StoreManager._
  import context.dispatcher

  val zk = Zookeeper(context.system).client
  val cs = Cassandra(context.system).cluster

  var stores: StoreMap = StoreMap(Map.empty, Map.empty)
  var ops: Map[StoreModification,ActorRef] = Map.empty

  override def preStart() {
    getStores pipeTo self
    log.debug("started {}", self.path.name)
  }

  def receive = {

    /* the getStores future has returned with the current stores list */
    case _stores: StoreMap =>
      stores = _stores
      context.parent ! _stores

    /* start async store create operation */
    case op: CreateStore =>
      if (ops.contains(op)) {
        sender ! StoreModificationFailed(new Exception("operation %s is already in progress".format(op)), op)
      } else {
        ops = ops + (op -> sender)
        createStore(op) pipeTo self
      }

    /* process the async store create result */
    case result @ CreatedStore(op: CreateStore, store: Store) =>
      ops.get(op) match {
        case Some(caller) =>
          caller ! result
          ops = ops - op
        case None =>  // do nothing
      }
      val storesById = stores.storesById ++ Map(store.id -> store)
      val storesByName = stores.storesByName ++ Map(store.name -> store)
      self ! StoreMap(storesById, storesByName)

    /* start async store delete operation */
    case op: DeleteStore =>
      if (ops.contains(op)) {
        sender ! StoreModificationFailed(new Exception("operation %s is already in progress".format(op)), op)
      } else {
        stores.storesById.get(op.id) match {
          case Some(store: Store) =>
            ops = ops + (op -> sender)
            deleteStore(store, op) pipeTo self
          case None =>
            sender ! StoreModificationFailed(new Exception("no such store %s".format(op.id)), op)
        }
      }

    /* process the async store delete result */
    case result @ DeletedStore(op: DeleteStore, store: Store) =>
      ops.get(op) match {
        case Some(caller) =>
          caller ! result
          ops = ops - op
        case None =>  // do nothing
      }
      val storesById = stores.storesById - store.id
      val storesByName = stores.storesByName - store.name
      self ! StoreMap(storesById, storesByName)

    /* describe the specified store */
    case DescribeStore(id) =>
      stores.storesById.get(id) match {
        case Some(store) =>
          sender ! StoreStatistics(store.id, store.name, store.created)
        case None =>
          sender ! ResourceNotFound
      }

    /* describe the store with the specified name, if it exists */
    case FindStore(name) =>
      stores.storesByName.get(name) match {
        case Some(store) =>
          sender ! StoreStatistics(store.id, store.name, store.created)
        case None =>
          sender ! ResourceNotFound
      }

    /* describe all stores */
    case EnumerateStores =>
      val stats = stores.storesById.values.map {store =>
        StoreStatistics(store.id, store.name, store.created)
      }.toSeq
      sender ! EnumeratedStores(stats)

    /* operation failed */
    case failure @ StoreModificationFailed(cause: Throwable, op: StoreModification) =>
      log.error("operation {} failed: {}", op, cause.getMessage)
      ops.get(op) match {
        case Some(caller) =>
          caller ! failure
          ops = ops - op
        case None =>  // do nothing
      }
  }

  /**
   * Asynchronously retrieve the list of stores.
   */
  def getStores = Future[StoreMap] {
    zk.checkExists().forPath("/stores") match {
      case stat: Stat =>
        val znodes = zk.getChildren.forPath("/stores")
        log.debug("found {} stores in /stores", znodes.length)
        val storesById: Map[String,Store] = znodes.map { storeNode =>
          val storePath = "/stores/" + storeNode
          val name = storeNode
          val id = new String(zk.getData.forPath(storePath), Zookeeper.UTF_8_CHARSET)
          val createdString = new String(zk.getData.forPath(storePath + "/created"), Zookeeper.UTF_8_CHARSET)
          val created = new DateTime(createdString.toLong, DateTimeZone.UTC)
          val store = Store(id, name, created)
          log.debug("found store {}", store)
          (store.id, store)
        }.toMap
        val storesByName = storesById.values.map(store => (store.name, store)).toMap
        StoreMap(storesById, storesByName)
      case null =>
        StoreMap(Map.empty, Map.empty)
    }
  }

  /**
   * Asynchronously create a store, or return the store if one exists with the
   * specified name.
   */
  def createStore(op: CreateStore) = Future[StoreModificationResult] {
    val path = "/stores/" + op.name
    /* lock store */
    try {
      val lock = new InterProcessReadWriteLock(zk, "/lock" + path)
      val writeLock = lock.writeLock()
      writeLock.acquire()
      try {
        /* check whether store exists */
        zk.checkExists().forPath(path) match {
          case stat: Stat =>
            val id = new String(zk.getData.forPath(path), Zookeeper.UTF_8_CHARSET)
            val createdString = new String(zk.getData.forPath(path + "/created"), Zookeeper.UTF_8_CHARSET)
            val created = new DateTime(createdString.toLong, DateTimeZone.UTC)
            log.debug("found store {} => {}", op.name, id)
            CreatedStore(op, Store(id, op.name, created))
          case null =>
            val id = new UUIDLike(UUID.randomUUID())
            val created = DateTime.now(DateTimeZone.UTC)
            /* create the keyspace in cassandra */
            val opts = new java.util.HashMap[String,String]()
            opts.put("replication_factor", "1")
            val ksDef = cs.makeKeyspaceDefinition()
              .setName(id.toString)
              .setStrategyClass("SimpleStrategy")
              .setStrategyOptions(opts)
              .addColumnFamily(cs.makeColumnFamilyDefinition()
              .setName("events")
              .setKeyValidationClass("UUIDType")
              .setComparatorType("UTF8Type"))
              .addColumnFamily(cs.makeColumnFamilyDefinition()
              .setName("meta")
              .setKeyValidationClass("UTF8Type")
              .setComparatorType("CompositeType(UTF8Type,UUIDType)"))
            val result = cs.addKeyspace(ksDef)
            log.debug("added keyspace {} (schema result id {})", id.toString, result.getResult.getSchemaId)
            /* create the store in zookeeper */
            zk.inTransaction()
              .create().forPath("/stores")
              .and()
              .create().forPath(path, id.toString.getBytes(Zookeeper.UTF_8_CHARSET))
              .and()
              .create().forPath(path + "/fields")
              .and()
              .create().forPath(path + "/created", created.getMillis.toString.getBytes(Zookeeper.UTF_8_CHARSET))
              .and()
              .commit()
            log.debug("created store {} => {}", op.name, id)
            CreatedStore(op, Store(id.toString, op.name, created))
        }
      } finally {
        /* unlock store */
        writeLock.release()
      }
    } catch {
      case ex: Throwable => StoreModificationFailed(ex, op)
    }
  }

  /**
   * Asynchronously delete a store.
   */
  def deleteStore(store: Store, op: DeleteStore) = Future[StoreModificationResult] {
    val path = "/stores/" + store.name
    /* lock store */
    try {
      val lock = new InterProcessReadWriteLock(zk, "/lock" + path)
      val writeLock = lock.writeLock()
      writeLock.acquire()
      try {
        DeletedStore(op, store)
      } finally {
        /* unlock store */
        writeLock.release()
      }
    } catch {
      case ex: Throwable => StoreModificationFailed(ex, op)
    }
  }

}

object StoreManager {

  def props() = Props[StoreManager]

  sealed trait StoreModification
  sealed trait StoreModificationResult
  case class CreateStore(name: String) extends StoreModification
  case class CreatedStore(op: CreateStore, result: Store) extends StoreModificationResult
  case class DeleteStore(id: String) extends StoreModification
  case class DeletedStore(op: DeleteStore, result: Store) extends StoreModificationResult
  case class StoreModificationFailed(cause: Throwable, op: StoreModification) extends StoreModificationResult

  case object EnumerateStores
  case class EnumeratedStores(stores: Seq[StoreStatistics])
  case class FindStore(name: String)
  case class DescribeStore(id: String)
  case class StoreStatistics(id: String, name: String, created: DateTime)

  sealed trait StoreNotification
  case class StoreMap(storesById: Map[String,Store], storesByName: Map[String,Store]) extends StoreNotification
}

case class Store(id: String, name: String, created: DateTime)
