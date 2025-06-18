
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "tp",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.5",
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "org.apache.spark" %% "spark-hive" % "3.5.5",

      "com.mysql" % "mysql-connector-j" % "8.4.0"
    ),
    // Fork JVM to avoid classpath conflicts
    fork := true,
    // Set JVM options for standalone Spark
    javaOptions ++= Seq(
      "-Dspark.master=spark://localhost:7077",
      "-Dspark.sql.warehouse.dir=data/spark-warehouse",
      "-Dspark.sql.hive.metastore.jars=builtin",
      "-Dspark.sql.legacy.timeParserPolicy=LEGACY"


),

    // Set main class
    Compile / mainClass := Some("Main")
  )