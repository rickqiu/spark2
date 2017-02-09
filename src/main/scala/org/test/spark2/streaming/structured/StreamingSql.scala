package org.test.spark2.streaming.structured
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.SparkSession

object StreamingSql {
  case class Person(name: String, city: String, country: String, age: Option[Int])

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("StreamingSql").getOrCreate()
    import spark.implicits._

    // create schema for parsing data
    val caseSchema = (ScalaReflection
      .schemaFor[Person]
      .dataType
      .asInstanceOf[StructType])

    val peopleStream = (spark.readStream
      .schema(caseSchema)
      .option("header", true) // Headers are matched to Person properties
      .option("maxFilesPerTrigger", 1) // each file is read in a separate batch
      .csv("data/people/") // load a CSV file
      .as[Person])

    // val query = peopleStream.select($"country", $"age")
    //  .groupBy($"country")
    //  .agg(avg($"age"))
      
    // SQL query
    peopleStream.createOrReplaceTempView("peopleTable")
    val query = spark.sql("SELECT country, avg(age) FROM peopleTable GROUP BY country")
   

    query.writeStream
      .outputMode("complete") // write results to screen
      .format("console")
      .start.awaitTermination()

    spark.stop()
  }  
}