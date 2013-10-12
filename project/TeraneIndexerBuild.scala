import com.github.retronym.SbtOneJar
import com.typesafe.sbt.SbtSite
import com.typesafe.sbt.SbtGhPages
import sbt._
import Keys._

object TeraneIndexerBuild extends Build {

  val teraneVersion = "0.1-SNAPSHOT"
  val akkaVersion = "2.2.1"
  val sprayVersion = "1.2-20130712"
  val astyanaxVersion = "1.56.37"
  val curatorVersion = "1.3.3"

  lazy val teraneIndexerBuild = Project(
    id = "terane-indexer",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      exportJars := true,
      name := "terane-indexer",
      version := teraneVersion,
      scalaVersion := "2.10.2",
      resolvers ++= Seq(
        "spray repo" at "http://nightlies.spray.io"
      ),
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-agent" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "com.netflix.astyanax" % "astyanax-core" % astyanaxVersion,
        "com.netflix.astyanax" % "astyanax-thrift" % astyanaxVersion,
        "com.netflix.astyanax" % "astyanax-cassandra" % astyanaxVersion,
        "com.netflix.curator" % "curator-recipes" % curatorVersion,
        "com.netflix.curator" % "curator-x-discovery" % curatorVersion,
        "io.spray" % "spray-can" % sprayVersion,
        "io.spray" % "spray-routing" % sprayVersion,
        "io.spray" %% "spray-json" % "1.2.4",
        "org.mapdb" % "mapdb" % "0.9.5",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "dnsjava" % "dnsjava" % "2.1.1",
        "com.twitter" %% "algebird-core" % "0.2.0",
        "nl.grons" %% "metrics-scala" % "3.0.3",
        "com.typesafe" %% "scalalogging-log4j" % "1.0.1",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      )
    ) ++ SbtOneJar.oneJarSettings
      ++ SbtSite.site.settings
      ++ SbtSite.site.sphinxSupport()
  )
}
