package com.syntaxjockey.terane.indexer

import akka.actor._
import com.typesafe.config._
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.io.{FileOutputStream, FileInputStream, File}
import java.nio.file.{Files, FileSystems}
import java.util.UUID

import com.syntaxjockey.terane.indexer.source.SourceSettings
import com.syntaxjockey.terane.indexer.sink.SinkSettings
import com.syntaxjockey.terane.indexer.http.HttpSettings
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperSettings
import com.syntaxjockey.terane.indexer.cassandra.CassandraSettings
import com.typesafe.config.ConfigException.WrongType

/**
 *
 */
class IndexerConfigExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[IndexerConfigExtension])

  val config = system.settings.config.getConfig("terane")

  /* build the settings tree */
  val settings = try {
    /* parse sources settings */
    val sourceSettings: Map[String,SourceSettings] = if (config.hasPath("sources")) {
      config.getConfig("sources").root().map { case (name: String, configValue: ConfigValue) =>
        name -> SourceSettings.parse(configValue.asInstanceOf[ConfigObject].toConfig)
      }.toMap
    } else Map.empty

    /* parse sinks settings */
    val sinkSettings: Map[String,SinkSettings] = if (config.hasPath("sinks")) {
      config.getConfig("sinks").root().map { case (name: String, configValue: ConfigValue) =>
        name -> SinkSettings.parse(configValue.asInstanceOf[ConfigObject].toConfig)
      }.toMap
    } else Map.empty

    /* parse http settings */
    val httpSettings = if (config.hasPath("http")) Some(HttpSettings.parse(config.getConfig("http"))) else None

    /* parse zookeeper settings */
    val zookeeperSettings = ZookeeperSettings.parse(config.getConfig("zookeeper"))

    /* parse cassandra settings */
    val cassandraSettings = CassandraSettings.parse(config.getConfig("cassandra"))

    /* get the node UUID */
    val nodeId = {
      val file = new File("nodeid")
      /* if the 'nodeid' file exists, then read the UUID from it */
      if (Files.exists(file.toPath)) {
        val istream = new FileInputStream(file)
        val bytes = new Array[Byte](36)
        try {
          val nread = istream.read(bytes)
          UUID.fromString(new String(bytes))
        } finally {
          istream.close()
        }
      }
      /* otherwise if the file does not exist, then generate a new UUID and write it out */
      else {
        val nodeId = UUID.randomUUID()
        val ostream = new FileOutputStream(file)
        try {
        ostream.write(nodeId.toString.getBytes)
        } finally {
          ostream.close()
        }
        nodeId
      }
    }
    log.debug("node UUID is " + nodeId.toString)

    IndexerConfigSettings(nodeId, sourceSettings, sinkSettings, httpSettings, zookeeperSettings, cassandraSettings)

  } catch {
    case ex: IndexerConfigException =>
      throw ex
    case ex: ConfigException =>
      throw IndexerConfigException(ex)
    case ex: Throwable =>
      throw IndexerConfigException("unexpected exception while parsing configuration", ex)
  }
}

object IndexerConfig extends ExtensionId[IndexerConfigExtension] with ExtensionIdProvider {
  override def lookup() = IndexerConfig
  override def createExtension(system: ExtendedActorSystem) = new IndexerConfigExtension(system)

  /* retrieve the IndexerConfigSettings from the actor system */
  def settings(implicit system: ActorSystem): IndexerConfigSettings = super.get(system).settings

  /* build the runtime configuration */
  val config = try {
    val baseConfig = ConfigFactory.load()
    val teraneConfig = loadConfigFile(baseConfig)
    ConfigFactory.defaultOverrides.withFallback(teraneConfig.withFallback(baseConfig))
  } catch {
    case ex: IndexerConfigException =>
      throw ex
    case ex: ConfigException =>
      throw IndexerConfigException(ex)
    case ex: Throwable =>
      throw IndexerConfigException("unexpected exception while parsing configuration", ex)
  }

  /**
   * load the terane config file from the location specified by the terane.config.file config
   * value in baseConfig.  this config value can be a string (e.g. from system property -Dterane.config.file)
   * or a string list.
   */
  def loadConfigFile(baseConfig: Config): Config = {
    val possibleConfigFiles = try {
      if (!baseConfig.hasPath("terane.config.file"))
        throw IndexerConfigException("terane.config.file is not specified")
      baseConfig.getStringList("terane.config.file").map(new File(_))
    } catch {
      case ex: WrongType =>
        Seq(new File(baseConfig.getString("terane.config.file")))
    }
    var config: Option[Config] = None
    for (file <- possibleConfigFiles if config.isEmpty) {
      if (file.canRead)
        config = Some(ConfigFactory.parseFile(file))
    }
    config.getOrElse(throw IndexerConfigException("failed to find a readable config file"))
  }
}


case class IndexerConfigSettings(
  nodeId: UUID,
  sources: Map[String,SourceSettings],
  sinks: Map[String,SinkSettings],
  http: Option[HttpSettings],
  zookeeper: ZookeeperSettings,
  cassandra: CassandraSettings)

case class IndexerConfigException(message: String, cause: Throwable) extends Exception(message, cause)

object IndexerConfigException {
  def apply(message: String): IndexerConfigException = new IndexerConfigException(message, null)
  def apply(cause: ConfigException): IndexerConfigException = IndexerConfigException("failed to parse config: " + cause.getMessage, cause)
}