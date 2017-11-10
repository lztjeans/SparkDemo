
//import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkContext
import org.apache.spark._


object Main {

  def createContext(appName:String) :SparkSession= {
    return SparkSession.builder().appName(appName).getOrCreate()
  }

  def createContext(appName:String, master:String): SparkContext={
    return new SparkContext(new SparkConf().setAppName(appName).setMaster(master))
  }

  def main(args: Array[String]): Unit = {
    val appName: String = "HelloWorld"
    val master: String = "local"
//    val master: String = "spark://localhost:18080"

//    val conf = new SparkConf().setAppName(appName).setMaster(master)
//    val sc = new SparkContext(conf)
//    val sc = SparkSession.builder.appName(appName).getOrCreate()

    val sc = createContext(appName,master)
//    val sc = createContext(appName)

    val inputPath = "file:///tmp/testFiles/*.txt"
    val outputPath = "file:///tmp/res"


    test1(sc, inputPath, outputPath)

//    val textFile = sc.read.textFile(inputPath)//dataset
    //val wordMap = textFile.flatMap(line => line.split(" ")).map(word => (word,1))
//    println(textFile)


  }

  private def test1(sc: SparkContext, inputPath: String, outputPath: String) = {
    val textFile = sc.textFile(inputPath)
    val counts = textFile
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(outputPath)

  }


}
