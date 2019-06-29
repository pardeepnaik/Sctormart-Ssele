package net.sansa_stack.template.spark.stats

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
// $example off$

object GradientBoostingClassification {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("GradientBoostingClassification")
      .getOrCreate()
//    val conf = new SparkConf().setAppName("GradientBoostedTreesClassificationExample")
//    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data file.
    
    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/Training/*")
    val newData = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/Eval_Cluster/part-00002")
    
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 5 
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification GBT model:\n ${model.toDebugString}")
    
    val Predictions = newData.map { point =>
      val pred = model.predict(point.features)
      (point.label, pred)
//      println(s"Pred = $pred")
    }
    Predictions.foreach(f => println("True lable: " + f._1, "Predicted label: " + f._2))
    val newTestErr = Predictions.filter(r => r._1 != r._2).count().toDouble / newData.count()
    println(s"New Test Error = $newTestErr")
    
    // Save and load model
   // model.save(sparkSession.sparkContext, "target/tmp/myGradientBoostingClassificationModel")
   // val sameModel = GradientBoostedTreesModel.load(sparkSession.sparkContext, "target/tmp/myGradientBoostingClassificationModel")
    // $example off$

    sparkSession.stop()
  }
}