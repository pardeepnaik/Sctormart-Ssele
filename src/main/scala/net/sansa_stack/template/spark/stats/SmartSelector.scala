////////////////////////////////////////////////
// Authors: Pardeep Kumar Naik                //
// Created on: 12/05/2018                     //
// Version: 0.0.1                             //
// Smart Selector                             //
////////////////////////////////////////////////

package net.sansa_stack.template.spark.stats
import java.net.URI
import scala.collection.mutable
import scala.collection.immutable.Map
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.sql.functions._
import java.net.{ URI => JavaURI }
import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import java.io.{BufferedWriter, FileWriter}
import scopt.OptionParser
import org.apache.log4j.Logger

object SmartSelector {
  var newTrainingDF: DataFrame = _
  var updatedTrainingDF: DataFrame = _
          
  var trainingDF: DataFrame = _
  def main(args: Array[String]):Unit = 
  {
    @transient lazy val consoleLog: Logger = Logger.getLogger(getClass.getName)
    
  parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        consoleLog.warn(parser.usage)
    }
  }
    
  case class Config(in: String = "")
   
   val parser: OptionParser[Config] = new scopt.OptionParser[Config]("SANSA - Smart Selector") {
    head("Recommending RDF Partitioning")

    //input file path
    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains RDF data (in N-Triples format)")

    //number of partition
   
  }
  
  def run(input: String): Unit = {
    println("====================================================")
    println("|                 Smart Selector                   |")
    println("====================================================")
    var partition1Score = 0
    var partition2Score = 0
   
    //initializing the spark session locally
    val spark = SparkSession.builder
          .master("spark://172.18.160.16:3090")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("SmartSelector")
          .getOrCreate()
          
//          
//    def getListOfFiles(dir: String):List[File] = 
//    {
//      val path = new File("/media/pardeep/1EE5-18A5/Evaluation/SANSA-Template-Maven-Spark-develop/src/main/resources/Evaluation")
//      if (path.exists && path.isDirectory) 
//      {
//        path.listFiles.filter(_.isFile).toList
//      } 
//      else 
//      {
//        List[File]()
//      }
//    }
     // val files = "/media/pardeep/1EE5-18A5/Evaluation/SANSA-Template-Maven-Spark-develop/src/main/resources/Evaluation"
     // val input = ""
     // for (input <- files)
     // {  
        println(input)
        val triplesRDD = NTripleReader.load(spark, JavaURI.create(input.toString()))
        partition1Score = 0
        partition2Score = 0
        
        val Obj1 = new RDFStatistics(triplesRDD,spark)
        val triples: Float = Obj1.Triples(triplesRDD)
        println("Number of Triples: " + triples)
        val distinctProperties: Float = Obj1.DistinctPredicates(triplesRDD)
        val distinctPropertiesRatio: Float = distinctProperties/triples
      println("Number of Distinct Properties: " + distinctProperties)
        
        distinctPropertiesRatio match
        {
          case x if x <= 1.94E-8 => partition1Score += 5
          case x if x <= 2.43E-5 => partition1Score += 4
          case x if x <= 4.86E-5 => partition1Score += 3
          case x if x <= 6.74E-4 => partition1Score += 2
          case x if x <= 1.3E-3 => partition1Score += 1
          case _ => partition1Score += 0
        }
        
        val Obj2 = new DistinctSubjects(triplesRDD,spark)
        val distinctSubjects: Float = Obj2.PostProc()
        val distinctSubjectsRatio: Float = distinctSubjects/triples
       println("Number of Distinct Subjects: " + distinctSubjects)
        
        distinctSubjectsRatio match 
        {
          case x if x <= 0.032 => partition2Score += 5
          case x if x <= 0.091 => partition2Score += 4
          case x if x <= 0.15 => partition2Score += 3
          case x if x <= 0.235 => partition2Score += 2
          case x if x <= 0.32 => partition2Score += 1
          case _ => partition2Score += 0
        }
        
        val blankSubjects: Float = Obj1.BlanksAsSubject(triplesRDD)
        println("Number of Blank Subjects: " + blankSubjects)
        
        if(blankSubjects > 0)
        {
          partition1Score += 5
        }
          
        println("\nVertical Partitioning Score: " + partition1Score)
        println("Semantic Partitioning Score: " + partition2Score + "\n")
        
        var selectedPartition = 0
        
        if(partition1Score >= partition2Score)
        {  
          selectedPartition = 0
        }
        else
        {  
          selectedPartition = 1
        }
        
        import spark.implicits._
         
//        if(input == files(0))
//        {
            trainingDF = Seq(
            (distinctPropertiesRatio, distinctSubjectsRatio, blankSubjects, selectedPartition)
            ).toDF("Distinct_Predicates", "Distinct_Subjects", "Blank Subjects", "Selected Partition")
//        } 
//        else
//        {    
//            newTrainingDF = Seq(
//            (distinctPropertiesRatio, distinctSubjectsRatio, blankSubjects, selectedPartition)
//            ).toDF("Distinct_Predicates", "Distinct_Subjects", "Blank Subjects", "Selected Partition")  
//            updatedTrainingDF = trainingDF.union(newTrainingDF)
//            trainingDF = updatedTrainingDF
//        }
        
        trainingDF.show()
//        var RDDtrainingDF = trainingDF.rdd
//        var pos = RDDtrainingDF.map(r => LabeledPoint(r.getInt(3), Vectors.dense(r.getFloat(0),r.getFloat(1),r.getFloat(2))))
//        val it = Iterator(pos.count.toInt)
//        MLUtils.saveAsLibSVMFile(pos, "src/main/resources/Eval/sample"+it.next())
       
      spark.stop
  }
  
}