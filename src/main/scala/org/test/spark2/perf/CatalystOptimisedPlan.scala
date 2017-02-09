package org.test.spark2.perf
import scala.math._
import org.apache.spark.sql.SparkSession

object CatalystOptimisedPlan {
  def main(arg: Array[String]) {

    val spark = SparkSession.builder().master("local[2]").config("spark.ui.port", 10200).appName("CatalystOptimisedPlan").getOrCreate()
    import spark.implicits._

    val data = spark.sparkContext.parallelize(1 to 10)

    val r = data
      .map(_ + 1)
      .map(_ + 1)
      .filter(_ > 1)
      .map(_ + 1)
      .filter(_ >= 2 * 4).cache

    //RDD lineage graph
    println(r.toDebugString)

    val r1 = r.collect().mkString(" ")
    println(r1)

    val query2 = data.toDS
      .select('value, 'value + 1 as 'value2)
      .select('value2 + 1 as 'value2)
      .filter('value2 > 1)
      .select('value2 + 1 as 'value2)
      .filter('value2 >= 2 * 4).cache()

    //Optimised plan
    query2.explain()

    val r2 = query2.collect().map(row => row(0)).mkString(" ")
    println(r2)

    spark.stop
  }
}