import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.io.File
import org.apache.log4j.{Level, Logger}

import java.util.Properties

object Gold {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("DB to Parquet ETL")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.sql.catalogImplementation", "hive")
      .enableHiveSupport()
      .getOrCreate()

    var df = spark.read.parquet("/tmp/spark-warehouse/charging_stations_partitioned")
    saveToMySQL(df,"stations_cleaned",spark)

    df = top_by_city(df, spark)
    saveToMySQL(df, "top_by_cities", spark)

  }

  def saveToMySQL(df: DataFrame, tableName: String, spark: SparkSession): Unit = {
    val totalRows = df.count()


    println("Saving DataFrame to MySQL...")
    // MySQL connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", "tonio")
    connectionProperties.put("password", "efrei1234")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // Additional MySQL connection properties
    connectionProperties.put("useSSL", "false")
    connectionProperties.put("allowPublicKeyRetrieval", "true")
    connectionProperties.put("serverTimezone", "UTC")
    connectionProperties.put("rewriteBatchedStatements", "true")
    connectionProperties.put("createDatabaseIfNotExist", "true")
    // Handle zero dates gracefully
    connectionProperties.put("zeroDateTimeBehavior", "convertToNull")

    // MySQL connection URL
    val jdbcUrl = "jdbc:mysql://localhost:3306/charging_stations"

    println(s"\nWriting cleaned dataset ($totalRows rows) to MySQL database...")

    // Write cleaned DataFrame to MySQL
    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, tableName, connectionProperties)

    println("Full dataset successfully loaded to MySQL!")
    println(s"Table $tableName created with $totalRows rows")
  }

  def top_by_city(df: DataFrame,spark: SparkSession): DataFrame = {
    val df_city = df.groupBy("consolidated_commune").agg(count("*").alias("count"))
    df_city.orderBy(desc("count"))
  }





}