import org.apache.spark.{SparkConf, SparkContext}

object buildingCount {
  def main(args : Array[String]) {
    val d = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get building count")
    val sc = new SparkContext(d)
    sc.setLogLevel("ERROR")

    val buldingcount = sc.textFile(args(1))
    val counts = buldingcount.flatMap(line => line.split(" "))
      .map(word => (word.equals("k=\"building\""), 1))
      .reduceByKey(_ + _)
      .map(word => "building: " + word._1 + " - count: " + word._2)

    counts.saveAsTextFile(args(2))
  }
}




