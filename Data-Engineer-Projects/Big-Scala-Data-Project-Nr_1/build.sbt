name := "log-analytics-pipeline"

version := "1.0.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"
val cassandraConnectorVersion = "3.5.0"
val kafkaVersion = "3.4.0"

libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // Spark Kafka Integration
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Spark Cassandra Connector
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.11",

  // JSON processing for configuration
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",

  // Web server for GUI
  "com.typesafe.akka" %% "akka-http" % "10.2.10",
  "com.typesafe.akka" %% "akka-stream" % "2.6.20",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.20",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.10",

  // JSON serialization
  "io.spray" %% "spray-json" % "1.3.6"
)

// Assembly plugin for creating fat JARs
enablePlugins(AssemblyPlugin)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Test configuration
Test / parallelExecution := false

// Resolve version conflicts
libraryDependencySchemes += "org.scala-lang.modules" %% "scala-parser-combinators" % VersionScheme.Always

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

// Java options for testing
Test / javaOptions ++= Seq(
  "-Xms512m",
  "-Xmx2048m",
  "-XX:+CMSClassUnloadingEnabled"
)