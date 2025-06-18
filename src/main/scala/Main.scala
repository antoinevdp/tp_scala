import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    try {
      println("Running Bronze...")
      Bronze.main(Array())

      println("Running Silver...")
      Silver.main(Array())

      println("Running Gold...")
      Gold.main(Array())

      println("All layers completed!")

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}