import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object MostUpdatedObject {
  def main(args : Array[String]) {

    val d = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get address street")
    val sc = new SparkContext(d)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("inputVersion/versionplanet.osm")

    val myval = df.select("_id","_version")

    import spark.implicits._
    val nextQuery = myval.orderBy($"_version".desc)


    nextQuery.show(1)

  }
}


