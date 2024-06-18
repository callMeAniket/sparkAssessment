
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "sparkAssessment"
  )
resolvers += "Akka library repository".at("https://repo.akka.io/maven")

lazy val akkaVersion = sys.props.getOrElse("akka.version", "2.9.3")
scalaVersion := "2.12.18" // Spark 3.2.x supports Scala 2.12.x

// Define Spark version that has better compatibility with newer Java versions
val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.apache.hadoop" % "hadoop-common" % "3.3.4",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.520",
  "com.typesafe" % "config" % "1.4.2",
  "com.github.jnr" % "jnr-posix" % "3.1.19",
  "joda-time" % "joda-time" % "2.12.7",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
)
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.6.3",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % "10.6.3",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.6.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => xs match {
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard // Custom strategy as an example
    case "module-info.class" :: Nil => MergeStrategy.concat
    case _ => MergeStrategy.discard // Or use other strategies as necessary
  }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
