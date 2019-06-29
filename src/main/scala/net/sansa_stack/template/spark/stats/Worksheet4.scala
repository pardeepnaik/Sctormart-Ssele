package net.sansa_stack.template.spark.stats

//import net.sansa_stack.template.spark.rdf
//package net.sansa_stack.query.spark.semantic
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.commons.io.FileUtils
import net.sansa_stack.rdf.spark.model._
import net.sansa_stack.rdf.spark.model.graph._
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.rdf.spark.partition._
import org.apache.jena.riot.Lang
import net.sansa_stack.query.spark.semantic._
import net.sansa_stack.rdf.spark.partition._
import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.spark.io._
import scopt.OptionParser
import org.apache.log4j.Logger
import java.net.{URI => JavaURI}
import java.lang.ReflectiveOperationException
import java.lang.Object

object Worksheet4 {
  def main(args: Array[String]): Unit = {
      @transient lazy val consoleLog: Logger = Logger.getLogger(getClass.getName)
    println("==================================================")
    println("|                 Smart Selector                 |")
    println("==================================================")

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.query)
      case None =>
        consoleLog.warn(parser.usage)
    }
  }
  
  case class Config(in: String = "", query: String = "")
   
   val parser: OptionParser[Config] = new scopt.OptionParser[Config]("SANSA - Smart Selector") {
    head("Recommending RDF Partitioning")

    //input file path
    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains RDF data (in N-Triples format)")

    //Jaccard similarity threshold value
    opt[String]('q', "query").required().
      action((x, c) => c.copy(query = x)).
      text("query path")

    //number of partition
   
  }
def run(input: String, queryPath: String): Unit = {

  val spark = SparkSession.builder().appName("SANSA examples").master("spark://172.18.160.16:3090").getOrCreate()
  val t1 = System.nanoTime
  //spark://172.18.160.16:3090
  ///////////////     Semantic Partitioning     ///////////////
  
//  val input = "/home/pardeep/testing/code/SANSA-Template-Maven-Spark/src/main/resources/cordis_links.nt"
  //val queryPath = "/media/pardeep/1EE5-18A5/Evaluation/SANSA-Template-Maven-Spark-develop/src/main/resources/Evaluation/query.sparql"
  val triples = spark.rdf(Lang.NTRIPLES)(input)
  

 // val triples=NTripleReader.load(spark, JavaURI.create(input))
  val partitionTriples = triples.partitionGraphAsSemantic()
  println("--------------------------------------------------------------------------------------------") 
  partitionTriples.take(10)foreach(println) 
  val result = partitionTriples.sparql(queryPath)
  println("Query Results:")
  result.take(10).foreach(println)
  val size = result.count()
  println("Number of Results:")
  println(size)
  
 ///////////////     Vertical Partitioning     ///////////////
  
// val input = "/media/pardeep/1EE5-18A5/Evaluation/SANSA-Template-Maven-Spark-develop/src/main/resources/Evaluation/transitive-redirects_en.nt"
// val sparqlQuery = """SELECT ?s ?o
//                      WHERE {?s <http://dbpedia.org/ontology/wikiPageRedirects> ?o}
//                      LIMIT 10"""
// val triples = spark.rdf(Lang.NTRIPLES)(input)
// val vp_part = triples.partitionGraph()
// val query = vp_part.sparql(sparqlQuery)
// query.take(10).foreach(println)

 
 val duration = (System.nanoTime - t1) / 1e9d
 println(duration)
}
}
