package org.test.spark2.ml

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.Vectors

object LinearRegressionDF {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("../regression.txt")
    
    val data = lines.map(_.split(",")).map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))
    
    // convert RDD to DF with column names
    import spark.implicits._
    val colNames = Seq("label", "features")
    val df = data.toDF(colNames: _*)

    val Array(trainingDF, testDF) = df.randomSplit(Array(0.6, 0.4))

    // create our linear regression model
    val lir = new LinearRegression()
      .setRegParam(0.01) // regularization 
      .setElasticNetParam(0.1) // elastic net mixing
      .setMaxIter(100) // max iterations
      .setTol(1E-6) // convergence tolerance

    val model = lir.fit(trainingDF)
    
    val predictions = model.transform(testDF).cache()

    val predictionAndLabel = predictions.select("prediction", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))

    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    val MSE = predictionAndLabel.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

    spark.stop()

  }
}