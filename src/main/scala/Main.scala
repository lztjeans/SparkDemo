import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {

  val appName: String = "HelloWorld"
  val master: String = "local"

  def reduceByKey : T ={}

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster(master)
//    val sparkSession = SparkSession.builder().appName(appName).config(conf).getOrCreate()
//    val sc = sparkSession.sparkContext
    //val sc = new sparkSession().


//    val conf = new SparkConf().setAppName(appName).setMaster(master)
//    val sc = new SparkContext(conf)

    val sc = SparkSession.builder.appName("Simple Application").getOrCreate()

    val textFile = sc.read.textFile("file:///home/Tim/dockerManu.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//    counts.saveAsTextFile("file:///home/Tim/dock2")

    //textFile.saveAsTextFile("file:///home/Tim/dock2")
    //sparkSession.close()




  }
}
