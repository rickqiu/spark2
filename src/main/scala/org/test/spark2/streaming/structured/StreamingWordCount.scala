package org.test.spark2.streaming.structured
import org.apache.spark.sql.SparkSession

object StreamingWordCount {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("StreamingWordCount").getOrCreate()
    import spark.implicits._

    // Load data
    val text = (spark.readStream
      .option("maxFilesPerTrigger", 1) // fake streaming read one file per trigger
      .text("data/othello/part*")
      .as[String])

    // Count words
    val counts = text.flatMap(line => line.split("\\s+"))
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