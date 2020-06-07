import java.io.{File}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.io.Directory


object Aufgabe10_Nicolas {
  val AppName:String = "aufgabe10"
  val Languages:List[String] = List("Dutch", "English", "French", "German", "Italian", "Russian", "Spanish", "Ukrainian")
  val AnalysisDir:String = "src/main/resources/analysis/"
  val ResultDir:String = "src/main/resources/result/"
  val StopWordsDir:String = "src/main/resources/stopwords/"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(AppName).setMaster("local[*]").set("spark.driver.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    //Deletes old Result directory so that Spark can write files
    val directory = new Directory(new File(ResultDir))
    directory.deleteRecursively()
    for(language <- Languages) { filterWordsForLanguage(language, sc) }
  }

  /**
   * Gets list of files from a specific directory
   * @param dir language directory
   * @return list of all files in that directory
   */
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /**
   * Filters the words from each file for a specific language
   * @param lang language
   * @param sc spark context
   */
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
          .subtractByKey(sc.makeRDD(Array(("",1)))) //remove flatMap => split entry of empty lines
          .sortBy(_._2, ascending = false)
        if (top10 == null) {
          top10 = counts
        } else {
          top10 = top10.union(counts)
        }
        counts.map(entry => s"${entry._1} : ${entry._2}").coalesce(1)
          .saveAsTextFile(ResultDir + lang + "/" + file.getName)
      }
    })

    val stopwords = sc.textFile(StopWordsDir + lang + ".txt").map(word => (word.toLowerCase, 1))
    top10.subtractByKey(stopwords).reduceByKey(_+_).sortBy(_._2, ascending = false)
      .zipWithIndex().filter(_._2 < 10).coalesce(1)
    .map(entry => s"#${entry._2}: ${entry._1._1} (${entry._1._2})").saveAsTextFile(ResultDir + "top10/" + lang)

  }
}