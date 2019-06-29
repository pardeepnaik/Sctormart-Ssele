package net.sansa_stack.template.spark.stats
//import org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
// $example off$
import org.apache.spark.sql.SparkSession

object RandomForestClassification {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("RandomForestClassification")
      .getOrCreate()
//    val spark = SparkSession
//      .builder
//      .appName("RandomForestClassifier")
//      .getOrCreate()

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
   val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/Training/*")
   val newData = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/Eval_Cluster/part-00000")
 
   // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 9 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification forest model:\n ${model.toDebugString}")
    
    val Predictions = newData.map { point =>
      val pred = model.predict(point.features)
      (point.label, pred)
    }
    Predictions.foreach(f => println("True lable: " + f._1, "Predicted label: " + f._2))
    val newTestErr = Predictions.filter(r => r._1 != r._2).count().toDouble / newData.count()
    println(s"New Test Error = $newTestErr")

    // Save and load model
//    model.save(sparkSession.sparkContext, "target/tmp/RandomForestClassificationModel")
//    val sameModel = RandomForestModel.load(sparkSession.sparkContext, "target/tmp/RandomForestClassificationModel")
    // $example off$

    sparkSession.stop()
  }
}