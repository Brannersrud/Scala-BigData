import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.size
import org.apache.spark.{SparkConf, SparkContext}
object WhichHighwayMostNodes {
  def main(args : Array[String]) {

    val d = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get address street")
    val sc = new SparkContext(d)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "way")
      .load("input/areaextract.osm")

    import spark.implicits._
    val myquery = df.select($"nd", $"_id", explode($"tag").as("Tags"))
    val filteredQuery = myquery.filter($"Tags._k" === "highway")
    val withCountQuery = filteredQuery.withColumn("count_nodes", size($"nd"))
    val sortedQuery = withCountQuery.orderBy($"count_nodes".desc)

    sortedQuery.show(20)



  }

}

