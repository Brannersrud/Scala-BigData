import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, max, min, expr}
import org.apache.spark.{SparkConf, SparkContext}
object LargestLatitudalExtent {

  def main(args: Array[String]) {
    val d = new SparkConf()
      .setMaster("local")
      .setAppName("Get address street")
    val sc = new SparkContext(d)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    val nodedf = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("input/areaextract.osm")

    val buildingdf = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("input/areaextract.osm")

    import spark.implicits._
    //Filtering building
    val nodeDf = nodedf.select($"_id", $"_lat",explode($"tag").as("buildingTags"))
    val filteredBuilding = nodeDf.filter($"buildingTags._k" === "building")
    //filtering name
    val buildingDf = buildingdf.select($"_id", explode($"tag").as("nameTags"))
    val filteredName = buildingDf.filter($"nameTags._k" === "name")

    val joined = filteredName.join(filteredBuilding, filteredName("_id") === filteredBuilding("_id"))

    val partition = Window.partitionBy($"nameTags._v")
    val high = joined.withColumn("Highest_lat", max($"_lat") over partition)
    val low = high.withColumn("Lowest_lat", min($"_lat") over partition)
    val res = low.withColumn("difference_in_latitude", expr("Highest_lat - Lowest_lat"))
    val sorted = res.orderBy($"difference_in_latitude".desc)

    sorted.show(1)
  }

}
