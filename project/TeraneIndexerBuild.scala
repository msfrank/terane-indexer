import com.github.retronym.SbtOneJar
import sbt._
import Keys._

object TeraneIndexerBuild extends Build {
  lazy val teraneIndexerBuild = Project(
    id = "terane-indexer",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      exportJars := true,
      name := "terane-indexer",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.1",
      resolvers += "spray repo" at "http://nightlies.spray.io",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % "2.2-M3",
        "io.spray" % "spray-can" % "1.1-20130509",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "nl.grons" %% "metrics-scala" % "2.2.0",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % "2.2-M3" % "test"
      )
    ) ++ SbtOneJar.oneJarSettings
  )
}
