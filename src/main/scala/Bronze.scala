import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.File
import java.util.Properties
import org.apache.log4j.{Level, Logger}

object Bronze {

  def main(args: Array[String]): Unit = {
    // Reduce Spark logging verbosity

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("DB to Parquet ETL")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()


    try {

      val df = readDB("stations",spark)
      // Process and partition data
      val outputPath = "data/parquet/stations"
      processAndSaveData(outputPath, df, spark)

      val path = "data/belib-points-de-recharge-pour-vehicules-electriques-donnees-statiques.csv"

      val parisDF = readCSV(path,spark)
      val parisOutputPath = "data/parquet/paris_stations"
      processAndSaveData(parisOutputPath, parisDF, spark)

    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def readDB(tableName: String,spark: SparkSession): DataFrame = {
    // Database connection properties
    val dbProperties = new Properties()
    dbProperties.put("user", "tonio")
    dbProperties.put("password", "efrei1234")
    dbProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    val jdbcUrl = "jdbc:mysql://localhost:3306/charging_stations"

    println("Reading data from database...")
    val df = spark.read
      .jdbc(jdbcUrl, tableName, dbProperties)

    df
  }

  def readCSV(csvPath: String,spark: SparkSession): DataFrame = {

    // Check if file exists
    val csvFile = new File(csvPath)
    if (!csvFile.exists()) {
      println(s"Error: CSV file not found at $csvPath")
      println(s"Current working directory: ${System.getProperty("user.dir")}")
      return null
    }
    println("Reading data from CSV file...")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .option("multiline", "true")
      .option("escape", "\"")
      .option("quote", "\"")
      .csv(csvPath)

    df
  }

  def processAndSaveData(outputPath: String, df: DataFrame, spark: SparkSession): Unit = {

    val dateColumnName = "date_mise_en_service"

    // If your date column is a string, convert it to date
    val processedDf = df.withColumn("partition_date",
        when(col(dateColumnName).isNotNull,
          to_date(col(dateColumnName), "yyyy-MM-dd"))
          .otherwise(current_date())
      )
      .withColumn("year", year(col("partition_date")))
      .withColumn("month", month(col("partition_date")))
      .withColumn("day", dayofmonth(col("partition_date")))

    // Show partitioning info
    println("Partitioning preview:")
    processedDf.select("partition_date", "year", "month", "day")
      .distinct()
      .orderBy(col("partition_date").desc)
      .show(10)

    // Save as Parquet with partitioning
    println(s"Saving data to Parquet format at: $outputPath")
    processedDf.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .option("compression", "snappy")
      .parquet(outputPath)

    println("Data successfully saved to Parquet format!")

    // Verify the saved data
    verifyParquetData(outputPath, spark)
  }

  def verifyParquetData(path: String, spark: SparkSession): Unit = {
    println("Verifying saved Parquet data...")

    val parquetDf = spark.read.parquet(path)

    println(s"Total records in Parquet: ${parquetDf.count()}")
    println("Partition structure:")

    // Show partition information
    parquetDf.select("year", "month", "day")
      .distinct()
      .orderBy("year", "month", "day")
      .show()

    println("Sample data from Parquet:")
    parquetDf.show(5)
  }
}