import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties
import java.io.File

object Feeder {
  def main(args: Array[String]): Unit = {

    // Create Spark session in standalone mode (no Hadoop required)
    val spark = SparkSession.builder()
      .appName("CSV to MySQL")
      .master("local[*]") // Use all available cores
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.sql.execution.arrow.pyspark.enabled", "false")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      // Disable Hadoop-related features
      .config("spark.sql.hive.metastore.jars", "builtin")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()

    // Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    try {
      println("Starting CSV to MySQL application...")
      println(s"Spark version: ${spark.version}")

      // Read CSV file
      val csvPath = "data/consolidation-etalab-schema-irve-statique-v-2.3.1-20250617.csv"

      // Check if file exists
      val csvFile = new File(csvPath)
      if (!csvFile.exists()) {
        println(s"Error: CSV file not found at $csvPath")
        println(s"Current working directory: ${System.getProperty("user.dir")}")
        return
      }

      println(s"Reading CSV file: $csvPath")
      val rawDf = spark.read
        .option("header", "true")
        .option("inferSchema", "true") // Read as strings first to handle data cleaning
        .option("delimiter", ",")
        .option("multiline", "true")
        .option("escape", "\"")
        .option("quote", "\"")
        .csv(csvPath)

      println(s"Raw dataset loaded successfully!")
      println(s"Number of rows: ${rawDf.count()}")
      println(s"Number of columns: ${rawDf.columns.length}")

      println("\nColumn names:")
      rawDf.columns.foreach(col => println(s"  - $col"))

      println("\nRaw schema:")
      rawDf.printSchema()

      // Clean the data - handle problematic datetime values
      val cleanedDf = rawDf.columns.foldLeft(rawDf) { (df, colName) =>
        // Check if column name suggests it's a date column
        if (colName.toLowerCase.contains("date") || colName.toLowerCase.contains("time")) {
          println(s"Cleaning date column: $colName")
          df.withColumn(colName,
            when(col(colName).rlike("^000[0-9].*") || col(colName) === "" || col(colName).isNull, null)
              .otherwise(col(colName))
          )
        } else {
          df
        }
      }

      // Show sample of cleaned data
      println("\nFirst 5 rows of cleaned data:")
      cleanedDf.show(5, truncate = false)

      // Check for any remaining problematic date values
      val dateColumns = cleanedDf.columns.filter(col => col.toLowerCase.contains("date") || col.toLowerCase.contains("time"))
      if (dateColumns.nonEmpty) {
        println("\nChecking date columns for problematic values:")
        dateColumns.foreach { colName =>
          val problematicCount = cleanedDf.filter(col(colName).rlike("^000[0-9].*")).count()
          if (problematicCount > 0) {
            println(s"  - $colName: Found $problematicCount problematic values (will be set to NULL)")
          } else {
            println(s"  - $colName: Clean")
          }
        }
      }

      val totalRows = cleanedDf.count()

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
      cleanedDf.write
        .mode(SaveMode.Overwrite)
        .jdbc(jdbcUrl, "stations", connectionProperties)

      println("Full dataset successfully loaded to MySQL!")
      println(s"Table 'stations' created with $totalRows rows")

    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getClass.getSimpleName}: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println("Cleaning up Spark session...")
      spark.stop()
    }
  }
}