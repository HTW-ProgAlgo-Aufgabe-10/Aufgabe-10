import org.apache.spark.sql.SparkSession

object Aufgabe10 {

  def main(args: Array[String]) {
    val logFile = "src/main/resources/Dutch/Baas Gansendonck - Hendrik Conscience.txt"
    val spark = SparkSession.builder.appName("Simple Application").config("spark.driver.host", "127.0.0.1").master("local[*]").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()

  }

}