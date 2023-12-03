import org.apache.spark.sql.SparkSession
object SimpleApp {
  def main(args: Array[String]) {
    // Should be some file on your system
    val logFile = "SimpleApp.scala" 
    // could be a file on hadoop hdfs
    // val logFile = "hdfs://hadoop-master:9000/data/SimpleApp.scala" 
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    // it is a dataset
    // you can use Spark-SQL or dataset API
    // just like you do it in the spark-shell
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
   spark.stop()
  }
}
