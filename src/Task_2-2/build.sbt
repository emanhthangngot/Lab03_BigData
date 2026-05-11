name         := "Task_2-2"
version      := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

// Use 'compile' scope so that 'sbt run' works in local mode without spark-submit.
// When packaging for spark-submit, change to "provided".
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion
)

Compile / mainClass := Some("Task2_2")

// Suppress verbose Ivy resolution logs
resolvers += "Apache Releases" at "https://repository.apache.org/content/repositories/releases/"
