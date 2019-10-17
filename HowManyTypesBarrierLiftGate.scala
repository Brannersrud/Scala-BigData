import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.{SparkConf, SparkContext}

object HowManyTypesBarrierLiftGate {
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


    val nodedf = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("input/areaextract.osm")

    import spark.implicits._
    //Way query for getting the nds
    val myquery = df.select($"nd", explode($"tag").as("Tags"))
    val filterNodeID=myquery.select($"Tags",explode($"nd").as("nodetags")).select($"nodetags._ref", $"Tags")
    val filteredQuery = filterNodeID.filter($"Tags._k" === "Highway" &&  $"Tags._v" === "road" || $"Tags._v" === "service" || $"Tags._v" === "unclassified" || $"Tags._v" === "path")
    //NodeQuery get the id and the values
    val nodeDf = nodedf.select($"_id", explode($"tag").as("Nodetags"))
    val filteredNodeQuery = nodeDf.filter($"Nodetags._k" === "barrier" && $"Nodetags._v" === "lift_gate")

    val join = filteredNodeQuery.join(filteredQuery, filteredNodeQuery("_id") === filteredQuery("_ref"))

    val finalizedquery = join.groupBy($"Tags._v").count()

    finalizedquery.show()
    // need to join on node ids ?
    


  }
}
