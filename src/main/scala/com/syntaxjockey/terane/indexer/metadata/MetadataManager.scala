package com.syntaxjockey.terane.indexer.metadata

import akka.actor.{Actor, ActorLogging}
import java.util.UUID
import com.syntaxjockey.terane.indexer.bier.matchers.TermMatcher.FieldIdentifier
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.CuratorFrameworkFactory
import scala.collection.JavaConversions._
import com.netflix.curator.framework.recipes.cache.PathChildrenCache
import org.joda.time.{DateTimeZone, DateTime}
import com.netflix.curator.utils.ZKPaths
import com.syntaxjockey.terane.indexer.bier.EventValueType
import com.syntaxjockey.terane.indexer.metadata.StoreManager.{Field, Store}
import java.nio.charset.Charset

/**
 * + namespace: String
 *  + "permissions"
 *    + permission: UUID
 *      + subject: UUID
 *      + "objects"
 *      + "capabilities"
 *  + "seeds"
 *    - seed: String
 *  + "workers"
 *    + worker: UUID -> name: String
 *      - "endpoint" -> String
 *      - "joined" -> Long
 *      - "can_index" -> Boolean
 *      - "can_query" -> Boolean
 *  + "stores"
 *    + store: UUID -> name: String
 *      - "created" -> Long
 *      + "fields"
 *        + field: UUID -> name: String
 *          - "type" -> String
 *          - "created" -> Long
 *          - "count" -> UUID
 *          - "frequency" -> UUID
 */
class MetadataManager extends Actor with ActorLogging with StoreManager {
  import MetadataManager._

  val config = context.system.settings.config.getConfig("terane.zookeeper")

  /* connect to zookeeper */
  val zkRetryPolicy = new ExponentialBackoffRetry(
    config.getMilliseconds("retry-sleep-time").toInt,
    config.getInt("retry-count"))
  log.debug("zkRetryPolicy = {}", zkRetryPolicy)
  val connectionString = config.getStringList("servers").mkString(",")
  val zkClient = CuratorFrameworkFactory.newClient(connectionString, zkRetryPolicy)
    .usingNamespace(config.getString("namespace"))
  log.debug("zkClient = {}", zkClient)
  zkClient.start()
  log.info("connecting to zookeeper servers {}", connectionString)

  /* synchronously load metadata */
  // FIXME: can we do this asynchronously?
  zkClient.getChildren.forPath("/stores").foreach { path =>
    /* load the store */
    val id = UUID.fromString(ZKPaths.getNodeFromPath(path))
    val storeName = zkClient.getData.forPath(path).mkString
    val created = new DateTime(zkClient.getData.forPath(path + "/created").mkString.toLong, DateTimeZone.UTC)
    /* load each field in the store */
    val fields = zkClient.getChildren.forPath(path + "/fields").map { path =>
      val id = UUID.fromString(ZKPaths.getNodeFromPath(path))
      val fieldName = zkClient.getData.forPath(path).mkString
      val fieldType = EventValueType.withName(zkClient.getData.forPath(path + "/type").mkString)
      val created = new DateTime(zkClient.getData.forPath(path + "/created").mkString.toLong, DateTimeZone.UTC)
      Field(id, fieldName, fieldType, created)
    }
    val fieldsById = fields.map(field => field.id -> field).toMap
    val fieldsByIdentifier = fields.map(field => FieldIdentifier(field.fieldName, field.fieldType) -> field).toMap
    val store = Store(id, storeName, created, fieldsById, fieldsByIdentifier)
    putStore(store)
    log.debug("loaded store {}", store)
    context.parent ! StoreCreated(store)
  }


  def receive = {

    case CreateStore(name, fields) =>
      val path = "/stores/" + name
      val id = UUID.randomUUID()
      val created = DateTime.now(DateTimeZone.UTC)
      zkClient.inTransaction()
        .create().forPath(path)
          .and()
        .create().forPath(path + "/fields")
          .and()
        .create().forPath(path + "/id", id.toString.getBytes(UTF_8_CHARSET))
          .and()
        .create().forPath(path + "/created", created.getMillis.toString.getBytes(UTF_8_CHARSET))
          .and()
        .commit()

    case CreateStoreField(store, name) =>
  }
}


object MetadataManager {
  val UTF_8_CHARSET = Charset.forName("UTF-8")
  case class CreateStore(name: String, fields: Option[Set[FieldIdentifier]])
  case class StoreCreated(store: Store)
  case class CreateStoreField(store: UUID, field: FieldIdentifier)
  case class StoreFieldCreated(store: Store, field: Field)
}
