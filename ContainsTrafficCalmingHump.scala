import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, explode}
import org.apache.spark.{SparkConf, SparkContext}
object ContainsTrafficCalmingHump {

  def main(args: Array[String]) {
    val d = new SparkConf()
      .setMaster("local")
      .setAppName("Get address street")
    val sc = new SparkContext(d)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "way")
      .load("input/areaextract.osm")


    val nodedf = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("input/areaextract.osm")

    import spark.implicits._
    //Way query for getting the nds
    val myquery = df.select($"_id".as("WayId") ,$"nd", explode($"nd").as("noderef")).select("noderef._ref", "WayId")
    //filtering and nodequery
    val nodeDf = nodedf.select($"_id", explode($"tag").as("Nodetags"))
    val filteredNodeQuery = nodeDf.filter($"Nodetags._k" === "traffic_calming" && $"Nodetags._v" === "hump")
    //joining on id
    val joined = filteredNodeQuery.join(myquery, filteredNodeQuery("_id") === myquery("_ref"))
    //partition to get count by each id
    val partition = Window.partitionBy($"WayId")
    val p = joined.withColumn("total_barrier_count", count($"Nodetags._k") over partition).orderBy($"total_barrier_count".desc)

    p.groupBy($"WayId", $"total_barrier_count").count().show(15)
  }
}
