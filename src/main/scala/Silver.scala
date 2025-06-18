import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.File

import org.apache.log4j.{Level, Logger}

object Silver {

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

    try {

      val filePath = "data/parquet/stations"
      val stationsDf = readParquet(filePath, spark)

      val parisFilePath = "data/parquet/paris_stations"
      val parisStationsDf = readParquet(parisFilePath, spark)

      val mergedDf = mergeDF(stationsDf,parisStationsDf,spark)

      val cleanedDf = cleanDF(mergedDf,spark)

      val totalRows = cleanedDf.count()

      println(s"Total rows in merged DataFrame: $totalRows")

      saveToHive(cleanedDf,spark)



    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def readParquet(filePath: String, spark: SparkSession): DataFrame = {
    val parquetFile = new File(filePath)
    if (!parquetFile.exists()) {
      println(s"Error: CSV file not found at $filePath")
      println(s"Current working directory: ${System.getProperty("user.dir")}")
      return null
    }

    println("Reading data from Parquet file...")
    val parquetDf = spark.read.parquet(filePath)
    parquetDf
  }

  def mergeDF(df1: DataFrame, df2: DataFrame, spark: SparkSession): DataFrame = {

    val df1Columns = df1.columns
    val df2ville = df2.withColumn("consolidated_commune", lit("Paris"))
    val df2Columns = df2ville.columns

    val commonCols = df1Columns.intersect(df2Columns)

    println("Selected common columns:")
    println(commonCols.mkString("Array(", ", ", ")"))


    var df1Filtered = df1.select(commonCols.head,commonCols.tail: _*)
    df1Filtered = df1Filtered.where("code_insee_commune != '75056' OR code_insee_commune IS NULL")


    val df2Filtered = df2ville.select(commonCols.head,commonCols.tail: _*)

    val targetSchema = df2Filtered.schema

    val targetTypes = targetSchema.fields.map(field => field.name -> field.dataType).toMap

    df1Filtered = df1Filtered.columns.foldLeft(df1Filtered) { (df, colName) =>
      targetTypes.get(colName) match {
        case Some(targetType) =>
          // Cast the column to the target type
          df.withColumn(colName, col(colName).cast(targetType))
        case None =>
          df
      }
    }

    df1Filtered.union(df2Filtered)
  }

  def cleanDF(df: DataFrame, sparkSession: SparkSession): DataFrame = {

    val cleaned = regexp_replace(col("coordonneesXY"), "\\[|\\]", "") // remove brackets

    val splitCoords = split(cleaned, ",\\s*") // split by comma and optional space

    val dfWithCoords = df
      .withColumn("lon", splitCoords.getItem(0).cast("double"))
      .withColumn("lat", splitCoords.getItem(1).cast("double"))

    dfWithCoords
  }

  def saveToHive(df: DataFrame, spark: SparkSession): Unit = {
    println("Saving DataFrame to Hive...")


    try {

      val tableName = "charging_stations_partitioned"


      df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("path", s"/tmp/spark-warehouse/$tableName")
        .saveAsTable(tableName)

      println(s"Successfully saved to partitioned Hive table: $tableName")


    } catch {
      case e: Exception =>
        println(s"Error saving to partitioned Hive table: ${e.getMessage}")
        e.printStackTrace()
    }

  }



}


