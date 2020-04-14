import java.io.File

import Aufgabe10_Nicolas.getListOfFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Aufgabe10_Nicolas {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("aufgabe10").setMaster("local[*]").set("spark.driver.host", "127.0.0.1")
      .set("spark.hadoop.orc.overwrite.output.file", "true")
    val sc = new SparkContext(conf)
    filterWordsForLanguage("German", sc)

  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def filterWordsForLanguage(lang: String, sc: SparkContext) : Unit = {
    val germanFiles = getListOfFiles("src/main/resources/analysis/" + lang)
    var top10 = null: RDD[(String, Int)]
    germanFiles.foreach(file => {
      if (file.getPath.split("\\.").last.equals("txt")) {
        val textFile = sc.textFile(file.getPath)
        val counts = textFile.flatMap(line => line.split("\\PL+"))
          .map(word => (word.toLowerCase, 1))
          .reduceByKey(_ + _)
          .sortBy(_._2, ascending = false)

        counts.coalesce(1)
          .saveAsTextFile("src/main/resources/result/" + lang + "/" + file.getName + "/" + System.currentTimeMillis())
        if (top10 == null) {
          top10 = counts
        } else {
          top10 = top10.union(counts)
        }
      }
    })
    top10.reduceByKey(_+_).sortBy(_._2, ascending = false)
      .zipWithIndex().filter(_._2 < 10).coalesce(1).saveAsTextFile("src/main/resources/result/top10/" + lang + "/" + System.currentTimeMillis())

  }
}