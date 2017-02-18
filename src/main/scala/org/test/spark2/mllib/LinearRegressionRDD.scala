package org.text.spark2.mllib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD,LinearRegressionModel}
import org.apache.spark.mllib.optimization.SquaredL2Updater


object LinearRegressionRDD {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "LinearRegressionRDD")
    val data = sc.textFile("data/regression.txt")
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
    
    val trainingData = training.map(LabeledPoint.parse).cache()
    val testData = test.map(LabeledPoint.parse)

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(1)
      .setUpdater(new SquaredL2Updater())
      .setRegParam(0.001)
      
      
    val model = algorithm.run(trainingData) 
    // val model = LinearRegressionModel.load(sc, "target/LinearRegressionWithSGD_Model")
   
    val predictions = model.predict(testData.map(_.features))
    val predictionAndLabel = predictions.zip(testData.map(_.label))

    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    val MSE = predictionAndLabel.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)
    
    // Save and load model
    model.save(sc, "target/LinearRegressionWithSGD_Model")
  }
}