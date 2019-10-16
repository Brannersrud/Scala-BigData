import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object streetAddressCount {

  def main(args : Array[String]) {

    val d = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get address street")
    val sc = new SparkContext(d)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "tag")
      .load("input/areaextract.osm")

    import spark.implicits._

    val myval = df.select("_k", "_v").filter($"_k" === "addr:street")

    val nextQuery = myval.groupBy("_v").count()

    nextQuery.show()







  }
}
