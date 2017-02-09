package org.test.spark2.streaming.dstream
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
/**
 *  * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 */
object NetworkWordCount {
  def main(args : Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val wc = lines.flatMap(_.split(" ")).map { word => (word, 1) }.reduceByKey(_ + _)
    wc.print()

    ssc.start()
    ssc.awaitTermination()
  } 
}