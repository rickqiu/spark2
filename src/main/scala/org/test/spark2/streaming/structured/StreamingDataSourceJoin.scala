package org.test.spark2.streaming.structured
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
object StreamingDataSourceJoin {
  case class User(id: Int, name: String, email: String, country: String)
  case class Transaction(userid: Int, product: String, cost: Double)

  
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("StreamingDataSourceJoin").getOrCreate()
    import spark.implicits._

    // A user dataset
    // Notice that we do not have to provide a schema
    // We can simply infer it
    val users = spark.read
      .option("inferSchema", "true")
      .option("header", true)
      .csv("data/customers.csv")
      .as[User]

    val transactionSchema = (ScalaReflection
      .schemaFor[Transaction]
      .dataType
      .asInstanceOf[StructType])

    // A stream of transactions
    val transactionStream = spark.readStream
      .schema(transactionSchema)
      .option("header", true)
      .option("maxFilesPerTrigger", 1)
      .csv("data/transactions/*.csv")
      .as[Transaction] 
      
 
    // Join transaction stream with user dataset
     val query = transactionStream
      .join(users, users("id") === transactionStream("userid"))
      .groupBy($"product")
      .agg(sum($"cost") as "spending")

    // Print result
    query.writeStream
      .outputMode("complete")
      .format("console")
      .start.awaitTermination()
      
    spark.stop()
  }
}