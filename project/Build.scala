import sbt._
import Keys._

/*

 "org.eclipse.jetty" % "jetty-server" % "7.5.3.v20111011",
 "org.scalatest" %% "scalatest" % "1.6.1" % "test",
 "org.scalacheck" %% "scalacheck" % "1.9" % "test",
 "com.novocode" % "junit-interface" % "0.8" % "test"

 */

object BuildSettings {
  val HADOOP_VERSION = "0.20.2-cdh3u4"
  val HADOOP_MAJOR_VERSION = "1"
  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.edb",
    version := "0.0.1",
    scalaVersion := "2.9.3",
    scalaHome := Some(file("/server/scala-2.9.3/")),
    scalacOptions ++= Seq(),
    //below are added for testing framework, copied from spark-0.7.2 Build.scala
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
      "com.novocode" % "junit-interface" % "0.9" % "test",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.easymock" % "easymock" % "3.1" % "test",
      "org.spark-project" %% "spark-core" % "0.7.3",
      "org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION excludeAll( ExclusionRule(organization = "org.codehaus.jackson") )
    ),
  resolvers ++= Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/")
)
  unmanagedBase <<= baseDirectory { base => base / "lib" }
}

/**
* root build
*/
object EDB_Build extends Build {
import BuildSettings._

/*lazy val root: Project = Project("root",
file("core"),
settings = buildSettings
) aggregate(parser, core) */

lazy val root: Project = Project("root",
file("core"),
settings = buildSettings
)


/*lazy val parser: Project = Project(
"parser",
file("parser"),
settings = buildSettings
) */

/*lazy val core: Project = Project(
"core",
file("core"),
settings = buildSettings
) dependsOn(parser) */

lazy val core: Project = Project(
"core",
file("core"),
settings = buildSettings
)

}
