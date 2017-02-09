package org.test.spark2.basic
import org.apache.spark.sql.SparkSession
object JoiningData {
  case class User(id: Int, name: String, email: String, country: String)
  case class Transaction(userid: Int, product: String, cost: Double)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("JoiningData").getOrCreate()
    import spark.implicits._

    val customers = (spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/customers.csv")
      .as[User])

    val transactions = (spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/transactions/*.csv")
      .as[Transaction])

    customers.join(transactions, customers.col("id") === transactions.col("userid"))
      .groupBy($"name")
      .sum("cost")
      .show

  }
}