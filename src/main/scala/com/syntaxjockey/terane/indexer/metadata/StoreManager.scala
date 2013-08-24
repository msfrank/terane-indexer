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

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import akka.actor.Status.Failure
import com.netflix.curator.framework.recipes.locks.InterProcessReadWriteLock
import org.apache.zookeeper.data.Stat
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import scala.concurrent.Future
import java.util.UUID

import com.syntaxjockey.terane.indexer.zookeeper.Zookeeper
import com.syntaxjockey.terane.indexer.cassandra.CassandraClient
import com.syntaxjockey.terane.indexer.UUIDLike

/**
 * + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      - "created" -> Long
 *      - "count" -> UUID
 *      + "fields"
 */
class StoreManager(cs: CassandraClient) extends Actor with ActorLogging {
  import StoreManager._
  import context.dispatcher

  var zk = Zookeeper(context.system).client

  var stores: StoresChanged = StoresChanged(Map.empty, Map.empty)
  getStores pipeTo self

  log.debug("started {}", self.path.name)

  def receive = {

    /* the getStores future has returned with the current stores list */
    case _stores: StoresChanged =>
      stores = _stores
      context.parent ! _stores

    case CreateStore(name) =>
      createStore(name) pipeTo self

    case CreatedStore(store) =>
      val storesById = stores.storesById ++ Map(store.id -> store)
      val storesByName = stores.storesByName ++ Map(store.name -> store)
      self ! StoresChanged(storesById, storesByName)

    case Failure(cause) =>
      log.debug("received failure: {}", cause.getMessage)
  }

  /**
   * Asynchronously retrieve the list of stores.
   *
   * @return
   */
  def getStores = Future[StoresChanged] {
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
        StoresChanged(storesById, storesByName)
      case null =>
        StoresChanged(Map.empty, Map.empty)
    }
  }

  /**
   * Asynchronously create a store, or return the store if one exists with the
   * specified name.
   *
   * @param name
   * @return
   */
  def createStore(name: String) = Future[CreatedStore] {
    val path = "/stores/" + name
    /* lock store */
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
          log.debug("found store {} => {}", name, id)
          CreatedStore(Store(id, name, created))
        case null =>
          val id = new UUIDLike(UUID.randomUUID())
          val created = DateTime.now(DateTimeZone.UTC)
          /* create the keyspace in cassandra */
          val opts = new java.util.HashMap[String,String]()
          opts.put("replication_factor", "1")
          val ksDef = cs.cluster.makeKeyspaceDefinition()
            .setName(id.toString)
            .setStrategyClass("SimpleStrategy")
            .setStrategyOptions(opts)
            .addColumnFamily(cs.cluster.makeColumnFamilyDefinition()
              .setName("events")
              .setKeyValidationClass("UUIDType")
              .setComparatorType("UTF8Type"))
            .addColumnFamily(cs.cluster.makeColumnFamilyDefinition()
              .setName("meta")
              .setKeyValidationClass("UUIDType")
              .setComparatorType("UTF8Type"))
          val result = cs.cluster.addKeyspace(ksDef)
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
          log.debug("created store {} => {}", name, id)
          CreatedStore(Store(id.toString, name, created))
      }
    } finally {
      /* unlock store */
      writeLock.release()
    }
  }
}

object StoreManager {

  case class Store(id: String, name: String, created: DateTime)

  sealed trait StoreOperation
  case class CreateStore(name: String) extends StoreOperation
  case class CreatedStore(store: Store)

  sealed trait StoreNotification
  case class StoresChanged(storesById: Map[String,Store], storesByName: Map[String,Store]) extends StoreNotification
}
