package com.syntaxjockey.terane.indexer.metadata

import akka.actor.{Actor, ActorLogging}
import java.util.UUID
import scala.collection.JavaConversions._
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future
import akka.pattern.pipe

/**
 * + namespace: String
 *  + "stores"
 *    + store: String -> id: UUIDLike
 *      - "created" -> Long
 *      - "count" -> UUID
 *      + "fields"
 */
class StoreManager(zk: ZookeeperClient) extends Actor with ActorLogging {
  import StoreManager._

  val config = context.system.settings.config.getConfig("terane.zookeeper")
  import context.dispatcher

  var stores: StoresChanged = _
  getStores pipeTo self

  log.debug("started {}", self.path.name)

  def receive = {

    /* the getStores future has returned with the current stores list */
    case _stores: StoresChanged =>
      stores = _stores
      context.parent ! _stores

    case CreateStore(name) =>
      val path = "/stores/" + name
      val id = UUID.randomUUID()
      val created = DateTime.now(DateTimeZone.UTC)
      zk.client.inTransaction()
        .create().forPath(path)
          .and()
        .create().forPath(path + "/fields")
          .and()
        .create().forPath(path + "/id", id.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
          .and()
        .create().forPath(path + "/created", created.getMillis.toString.getBytes(ZookeeperClient.UTF_8_CHARSET))
          .and()
        .commit()
  }

  /**
   * Asynchronously retrieve the list of stores.
   *
   * @return
   */
  def getStores = Future[StoresChanged] {
    val znodes = zk.client.getChildren.forPath("/stores")
    log.debug("found {} stores in /stores", znodes.length)
    val storesById: Map[String,Store] = znodes.map { storeNode =>
      val storePath = "/stores/" + storeNode
      val name = storeNode
      val id = zk.client.getData.forPath(storePath).mkString
      val created = new DateTime(zk.client.getData.forPath(storePath + "/created").mkString.toLong, DateTimeZone.UTC)
      val store = Store(id, name, created)
      log.debug("found store {}", store)
      (store.id, store)
    }.toMap
    val storesByName = storesById.values.map(store => (store.name, store)).toMap
    StoresChanged(storesById, storesByName)
  }
}

object StoreManager {

  case class Store(id: String, name: String, created: DateTime)

  sealed trait StoreOperation
  case class CreateStore(name: String) extends StoreOperation

  sealed trait StoreNotification
  case class StoresChanged(storesById: Map[String,Store], storesByName: Map[String,Store]) extends StoreNotification
}
