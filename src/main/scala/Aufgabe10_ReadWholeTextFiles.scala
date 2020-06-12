import java.io.File
import java.time.Duration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.io.Directory


object Aufgabe10_ReadWholeTextFiles {
  //Constants
  val AppName:String = "aufgabe10"
  val Languages = List("Dutch", "English", "French", "German", "Italian", "Russian", "Spanish", "Ukrainian")

  //File paths
  val ResourcesDir = "src/main/resources/"
  val AnalysisDir:String = ResourcesDir + "analysis/"
  val ResultDir:String = ResourcesDir + "result/allFilesSeperatedUsingWholeTextFiles/"
  val FrequencyDir:String = ResultDir + "frequency/"
  val Top10Dir:String = ResultDir + "top10/"
  val StopWordsDir:String = ResourcesDir +"stopwords/"

  var start = 0L
  var end = 0L
  def main(args: Array[String]) {
    start = System.nanoTime
    //Init spark
    val conf = new SparkConf().setAppName(AppName).setMaster("local[8]").set("spark.driver.host", "127.0.0.1").set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)

    //Clear result folders
    val resultFolder = new Directory(new File(ResultDir))
    resultFolder.deleteRecursively()

    //For each language filter words and create top10 list
   // for(language <- Languages) {
      val words = filterWordsForLanguage("testLang", sc)
      //if (!words.isEmpty()) createTop10("testLang", words, sc)
    //}
    end = System.nanoTime
    val duration = Duration.ofNanos(end - start)
    printf("%d Hours %d Minutes %d Seconds%n",
      duration.toHours(), duration.toMinutes() % 60, duration.getSeconds() % 60);
  }

  /**
   * Gets list of files from a specific directory
   * @param dir language directory
   * @return list of all files in that directory
   */
  def getListOfFiles(dir: String): List[File] = {
    //Get files of a directory
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
  def filterWordsForLanguage(lang: String, sc:SparkContext) : RDD[(String, Int)] = {
    //Get files for a language
    /*val files = getListOfFiles(AnalysisDir + lang)
    if(files.isEmpty) return sc.emptyRDD*/

    var lastLang = "";
    val files = sc.wholeTextFiles(AnalysisDir + "*/*.txt").collect()

    //Accumulated words for all files
    var allWords = null: RDD[(String, Int)]

    //Filter words for each file for a language
    files.zipWithIndex.foreach{case (file, index) => {
        //Get a file
        val textFile = sc.parallelize(List(file._2))

        //Filter words
        val counts = textFile.flatMap(line => line.split("\\PL+"))
          .map(word => (word.toLowerCase, 1))
          .reduceByKey(_ + _)
          .subtractByKey(sc.makeRDD(Array(("",1)))) //remove flatMap => split entry of empty lines
          .sortBy(_._2, ascending = false)

        //Save output
        counts.map(entry => s"${entry._1} : ${entry._2}").coalesce(1)
          .saveAsTextFile(FrequencyDir + getPathInformation(file._1).language + "/" + getPathInformation(file._1).filename)

        var test = files.length
        //Requires sequential reading of wholeTextFiles and well organized folders
        if((lastLang != "" && lastLang != getPathInformation(file._1).language) || files.length == index + 2 /* files.length starts at 1, index at 0*/ ){
          createTop10(lastLang, allWords, sc)
          allWords = null
        }
        lastLang = getPathInformation(file._1).language

        //Accumulate words for all files
        if (allWords == null) {
          allWords = counts
        } else {
          allWords = allWords.union(counts)
        }
    }}

    allWords
  }

  class PathInformation(var language: String = "", var filename: String = "")

  def getPathInformation(path:String): PathInformation ={

    val pathInformation = new PathInformation

    for(language <- Languages) {
      if(path.contains(language)){
        pathInformation.language = language
      }
    }

    val start = path.indexOf(pathInformation.language)
    val end = path.indexOf(".txt")

    pathInformation.filename = path.slice(start + pathInformation.language.length + 1, end)

    pathInformation
  }

  /**
   * Create top10 list from data
   * @param lang language
   * @param data DataFrame
   */
  def createTop10(lang: String, data:RDD[(String, Int)], sc:SparkContext) = {
    //Get stopwords
    val stopwords = sc.textFile(StopWordsDir + lang + ".txt").map(word => (word.toLowerCase, 1))

    //Create top10 list
    data.subtractByKey(stopwords).reduceByKey(_+_).sortBy(_._2, ascending = false)
      .zipWithIndex().filter(_._2 < 10).coalesce(1)
      .map(entry => s"#${entry._2}: ${entry._1._1} (${entry._1._2})").saveAsTextFile(Top10Dir + lang)
  }
}
