name := "Lab03_BigData"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.4.1"
val hadoopVersion = "3.3.4"

lazy val root = (project in file("."))
  .settings(
    name := "Lab03",
    // Common dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion
    ),
    // Define the source directories for the tasks
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_1-1" / "src" / "main" / "scala",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_1-2" / "src" / "main" / "scala",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_2-1" / "src" / "main" / "scala",
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "Task_2-2" / "src" / "main" / "scala"
  )
