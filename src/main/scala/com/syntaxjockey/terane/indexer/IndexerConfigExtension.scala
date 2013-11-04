package com.syntaxjockey.terane.indexer

import akka.actor._
import com.typesafe.config.{Config, ConfigObject, ConfigValue, ConfigFactory}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.io.File
import java.nio.file.{Files, FileSystems}

import com.syntaxjockey.terane.indexer.source.SourceSettings
import com.syntaxjockey.terane.indexer.sink.SinkSettings
import com.syntaxjockey.terane.indexer.http.HttpSettings
import com.syntaxjockey.terane.indexer.zookeeper.ZookeeperSettings
import com.syntaxjockey.terane.indexer.cassandra.CassandraSettings

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

    IndexerConfigSettings(sourceSettings, sinkSettings, httpSettings, zookeeperSettings, cassandraSettings)

  } catch {
    case ex: IndexerConfigException =>
      throw ex
    case ex: Throwable =>
      throw new IndexerConfigException("unexpected exception while parsing configuration", ex)
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
    val config = sys.props.get("terane.config.file") match {
      case Some(propConfFile) =>
        ConfigFactory.parseFile(new File(propConfFile))
      case None =>
        val confFilePath = FileSystems.getDefault.getPath("conf", "terane.conf")
        val rootFilePath = FileSystems.getDefault.getPath("terane.conf")
        if (Files.isReadable(confFilePath))
          ConfigFactory.parseFile(confFilePath.toFile)
        else if (Files.isReadable(rootFilePath))
          ConfigFactory.parseFile(rootFilePath.toFile)
        else ConfigFactory.empty()
    }
    config.withFallback(baseConfig).getConfig("terane")
  } catch {
    case ex: IndexerConfigException =>
      throw ex
    case ex: Throwable =>
      throw new IndexerConfigException("unexpected exception while parsing configuration", ex)
  }
}

case class IndexerConfigSettings(
  sources: Map[String,SourceSettings],
  sinks: Map[String,SinkSettings],
  http: Option[HttpSettings],
  zookeeper: ZookeeperSettings,
  cassandra: CassandraSettings)

class IndexerConfigException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}
