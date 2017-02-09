package org.test.spark2.streaming.structured
import org.apache.spark.sql.SparkSession
/**
 * This is a Spark Simple Kafka Consumer
 * For Spark structured streaming with Kafka data source and writing to Cassandra,
 * @See https://github.com/ansrivas/spark-structured-streaming
 */
object SimpleKafkaConsumer {
  def main(args: Array[String]) {
    if (args.length != 2){
      print("SimpleKafkaConsumer {broker-list} {topic}")
      System.exit(1)
    }

    val spark = SparkSession.builder().master("local[2]").appName("SimpleKafkaConsumer").getOrCreate()
    import spark.implicits._
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args(0))
      .option("subscribe", args(1))
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]

    // Count words
    val counts = ds1.flatMap(line => line.split("\\s+"))
      .groupByKey(_.toLowerCase)
      .count

    // Print counts
    val query = counts
      .orderBy($"count(1)" desc)
      .writeStream
      .outputMode("complete")
      .format("console").start()

    query.awaitTermination()
    spark.stop()
  }
}