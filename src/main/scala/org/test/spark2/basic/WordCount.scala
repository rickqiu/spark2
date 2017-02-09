package org.test.spark2.basic
import org.apache.spark.sql.SparkSession
/**
 * An example of using Dataset
 */
object WordCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").config("spark.ui.port", 10200).appName("WordCount").getOrCreate()
    import spark.implicits._

    val lines = (spark.read.text("data/part*").as[String])
    val counts = lines.flatMap(line => line.split("\\s+")).filter(word => !word.isEmpty()).groupByKey(_.toLowerCase).count.orderBy($"count(1)" desc).cache
    counts.show(20)
  }
}