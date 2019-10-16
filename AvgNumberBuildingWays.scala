import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, size, avg}


object AvgNumberBuildingWays {

  def main(args : Array[String]) {
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

    import spark.implicits._
    val myquery = df.select($"nd", $"_id", explode($"tag").as("Tags"))
    val filteredQuery = myquery.filter($"Tags._k" === "highway")
    val withCountQuery = filteredQuery.withColumn("count_nodes", size($"nd"))
    val sortedQuery = withCountQuery.agg(avg($"count_nodes").as("Average node count"))

    sortedQuery.show()
  }
}
