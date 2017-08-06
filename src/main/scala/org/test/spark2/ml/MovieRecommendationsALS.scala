package org.test.spark2.ml
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import scala.io.Codec
import scala.collection.mutable.ListBuffer
import java.nio.charset.CodingErrorAction
import scala.io.Source
import scala.collection.mutable.Map
import org.apache.spark.sql.functions.explode

object MovieRecommendationsALS {
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  
  var firstUserRatings = ListBuffer[Rating]()
 
  def parseRating(str: String): Rating = {
    val fields = str.split('\t')
    if (fields(0).toInt == 0){
      firstUserRatings +=  Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }
    
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  
  def loadMovieNames(path:String): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames =  Map[Int, String]()
    
    val lines = Source.fromFile(path).getLines()

    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ml.MovieRecommendationsALS").master("local[1]").getOrCreate()
     
    println("Loading movie names...")
    val nameDict = loadMovieNames("./data/ml-100k/u.item")
     
    import spark.implicits._
    val ratings = spark.read.textFile("data/ml-100k/u.data").map(parseRating).toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
     
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
     
    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse") 

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)

    println("\nRatings from user ID 0:")
    
    for (i <- 0 until 3){
      println(nameDict(firstUserRatings(i).movieId) + " score " + firstUserRatings(i).rating)     
    }
 
    println("\nTop 10 recommendations for user ID 0:")
    
    val result = userRecs.filter($"userId" === 0).select(explode($"recommendations"))
    
    for (row <- result){
      val a = row.toString().replace("[[", "").replace("]]", "").split(",")
      println(nameDict(a(0).toInt) + " score " + a(1))
    }
    
    spark.stop()
  } 
}