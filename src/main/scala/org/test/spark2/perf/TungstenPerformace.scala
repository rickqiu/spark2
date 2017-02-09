package org.test.spark2.perf
import scala.math._
import org.apache.spark.sql.SparkSession
/**
 * TungstenPerformace demonstrates a low memory usage by using Tungsten Engine. 
 * The memory usages can be viewed in SparkUI http://localhost:10200/storage/
 */
object TungstenPerformace {
  def main(arg: Array[String]) {

    val spark = SparkSession.builder().master("local[2]").config("spark.ui.port", 10200).appName("TungstenPerformace").getOrCreate()
    import spark.implicits._

    val million = spark.sparkContext.parallelize(0 until math.pow(10, 6).toInt)
    
    //RDD
    million.cache.count
    //Dataset
    million.toDS().cache.count

    Thread.sleep(1200000)
    spark.stop
  }
}