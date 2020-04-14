import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aufgabe10_Nicolas {
  val AppName:String = "aufgabe10"
  val Languages:List[String] = List("German")
  //val Languages:List[String] = List("Dutch", "English", "French", "German", "Italian", "Russian", "Spanish", "Ukrainian")
  val AnalysisDir:String = "src/main/resources/analysis/"
  val ResultDir:String = "src/main/resources/result/"
  val StopWordsDir:String = "src/main/resources/stopwords/"

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(AppName).setMaster("local[*]").set("spark.driver.host", "127.0.0.1")
      .set("spark.hadoop.orc.overwrite.output.file", "true")
    val sc = new SparkContext(conf)

    for(language <- Languages) { filterWordsForLanguage(language, sc) }
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
    val files = getListOfFiles(AnalysisDir + lang)
    if(files.isEmpty) return

    var top10 = null: RDD[(String, Int)]
    files.foreach(file => {
      if (file.getPath.split("\\.").last.equals("txt")) {
        val textFile = sc.textFile(file.getPath)
        val counts = textFile.flatMap(line => line.split("\\PL+"))
          .map(word => (word.toLowerCase, 1))
          .reduceByKey(_ + _)
          .sortBy(_._2, ascending = false)

        counts.coalesce(1)
          .saveAsTextFile(ResultDir + lang + "/" + file.getName + "/" + System.currentTimeMillis())
        if (top10 == null) {
          top10 = counts
        } else {
          top10 = top10.union(counts)
        }
      }
    })

    val stopwords = sc.textFile(StopWordsDir + lang + ".txt").map(word => (word.toLowerCase, 1))
    top10.subtractByKey(stopwords).reduceByKey(_+_).sortBy(_._2, ascending = false)
      .zipWithIndex().filter(_._2 < 10).coalesce(1).saveAsTextFile(ResultDir + "top10/" + lang + "/" + System.currentTimeMillis())

  }
}