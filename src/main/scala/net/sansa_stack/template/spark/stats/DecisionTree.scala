package net.sansa_stack.template.spark.stats
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.model.Predict

// $example off$

object DecisionTreeClassification {

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("DecisionTreeClassification")
//    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Decision Tree")
      .getOrCreate()


    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/Training/*")
    val newData = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/Eval_Cluster/part-00000")
    // Split the data into training and test sets (20% held out for testing)
  //  data.foreach(println)
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))
     
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 16

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    //  printf("Predicted label %.2f".format(prediction));
      
    }
    
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification tree model:\n ${model.toDebugString}")
    
    val Predictions = newData.map { point =>
      val pred = model.predict(point.features)
      (point.label, pred)
    }
    Predictions.foreach(f => println("True lable: " + f._1, "Predicted label: " + f._2))
    val newTestErr = Predictions.filter(r => r._1 != r._2).count().toDouble / newData.count()
    println(s"New Test Error = $newTestErr")
    
    // Save and load model
//    model.save(sparkSession.sparkContext, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sparkSession.sparkContext, "target/tmp/myDecisionTreeClassificationModel")
    // $example off$
  
    sparkSession.sparkContext.stop()
  }
}