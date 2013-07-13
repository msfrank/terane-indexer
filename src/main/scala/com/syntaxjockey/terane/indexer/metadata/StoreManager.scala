package com.syntaxjockey.terane.indexer.metadata

import akka.actor.{Actor, ActorLogging}
import java.util.UUID
import scala.collection.JavaConversions._
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future
import akka.pattern.pipe
import com.syntaxjockey.terane.indexer.UUIDLike
import com.netflix.curator.framework.recipes.locks.InterProcessReadWriteLock
import org.apache.zookeeper.data.Stat
import com.syntaxjockey.terane.indexer.sink.CassandraClient
import akka.actor.Status.Failure

/**
 * + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      - "created" -> Long
 *      - "count" -> UUID
 *      + "fields"
 */
class StoreManager(zk: ZookeeperClient, cs: CassandraClient) extends Actor with ActorLogging {
  import StoreManager._

  val config = context.system.settings.config.getConfig("terane.zookeeper")
  import context.dispatcher

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
    zk.client.checkExists().forPath("/stores") match {
      case stat: Stat =>
        val znodes = zk.client.getChildren.forPath("/stores")
        log.debug("found {} stores in /stores", znodes.length)
        val storesById: Map[String,Store] = znodes.map { storeNode =>
          val storePath = "/stores/" + storeNode
          val name = storeNode
          val id = new String(zk.client.getData.forPath(storePath), ZookeeperClient.UTF_8_CHARSET)
          val createdString = new String(zk.client.getData.forPath(storePath + "/created"), ZookeeperClient.UTF_8_CHARSET)
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
    val lock = new InterProcessReadWriteLock(zk.client, "/lock" + path)
    val writeLock = lock.writeLock()
    writeLock.acquire()
    try {
      /* check whether store exists */
      zk.client.checkExists().forPath(path) match {
        case stat: Stat =>
          val id = new String(zk.client.getData.forPath(path), ZookeeperClient.UTF_8_CHARSET)
          val createdString = new String(zk.client.getData.forPath(path + "/created"), ZookeeperClient.UTF_8_CHARSET)
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
          zk.client.inTransaction()
            .create().forPath("/stores")
              .and()
            .create().forPath(path, id.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
              .and()
            .create().forPath(path + "/fields")
              .and()
            .create().forPath(path + "/created", created.getMillis.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
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
