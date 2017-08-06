package org.test.spark2.mllib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.mllib.recommendation.{ALS,Rating}
import scala.collection.mutable.Map

object MovieRecommendationsALS {

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

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "mllib.MovieRecommendationsALS")

    println("Loading movie names...")
    val nameDict = loadMovieNames("./data/ml-100k/u.item")

    val data = sc.textFile("data/ml-100k/u.data")
    val ratings = data.map(x => x.split('\t')).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble)).cache()

    val rank = 20
    val numIterations = 20

    val model = ALS.train(ratings, rank, numIterations)

    val userID = args(0).toInt
    println("\nRatings for user ID " + userID + ":")

    val myRatings = ratings.filter(x => x.user == userID).collect()

    for (rating <- myRatings) {
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toString)
    }

    println("\nTop 10 recommendations:")
    val recommendations = model.recommendProducts(userID, 10)

    for (recommendation <- recommendations) {
      println(nameDict(recommendation.product.toInt) + " score " + recommendation.rating)
    }
  }
}