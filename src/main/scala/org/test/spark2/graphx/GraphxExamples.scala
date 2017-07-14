package org.test.spark2.graphx
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object GraphxExamples {
  
  case class Peep(name:String, age:Int)
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder.master("local[2]").appName(s"${this.getClass.getSimpleName}").getOrCreate()
    println("runLoveRelationExample .............")
    runLoveRelationExample(spark)
    
    /*
    println("runPersonnelRelationExample ........")   
    runPersonnelRelationExample(spark)
    println("runPageRankExample .................")
    runPageRankExample(spark)
    println("runTriangleCountingExample .........")
    runTriangleCountingExample(spark)
    println("runConnectedComponentsExample ......")
    runConnectedComponentsExample(spark)
    */

    spark.stop()
  }
  
  private def runLoveRelationExample(spark: SparkSession): Unit = {

    val sc = spark.sparkContext

    val nodeArray = Array(
      (1L, Peep("Kim", 23)), (2L, Peep("Pat", 31)),
      (3L, Peep("Jennifer", 52)), (4L, Peep("Kelly", 39)),
      (5L, Peep("Tom", 30)))

    val edgeArray = Array(
      Edge(2L, 1L, 7), Edge(2L, 4L, 2),
      Edge(3L, 2L, 4), Edge(3L, 5L, 3),
      Edge(4L, 1L, 1), Edge(5L, 3L, 9))
      
    val nodeRDD:RDD[(Long, Peep)] = sc.parallelize(nodeArray, 2)
    val edgeRDD:RDD[Edge[Int]] = sc.parallelize(edgeArray, 2)
    
    val g: Graph[Peep, Int] = Graph(nodeRDD, edgeRDD)
    
    val results = g.triplets.filter(t => t.attr > 7)
    
    for (triplet <- results.collect){
      println(s"${triplet.srcAttr.name} likes ${triplet.dstAttr.name}")
    }
  }
  
  private def runPersonnelRelationExample(spark: SparkSession): Unit = {
 
      val sc = spark.sparkContext
      
      val nodeArray = Array((3L, ("rxin", "student")),(7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")))
                       
      val edgeArray = Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"))
        
                       
                       
      val users: RDD[(VertexId, (String, String))] = sc.parallelize(nodeArray)
      val relationships: RDD[Edge[String]] = sc.parallelize(edgeArray)
      
      val defaultUser = ("John Doe", "Missing")
      
      val g:Graph[(String, String), String] = Graph(users, relationships, defaultUser)
      
      //find colleague relation
      val results = g.triplets.filter(triplet => triplet.attr.contains("colleague"))
      results.collect().foreach(println)
      
      //count all users who are postdoc
      val postdocCount = g.vertices.filter{ case (id, (name, pos)) => pos == "postdoc" }.count
      println("Postdoc count: " + postdocCount)
      
      //val advisorCount = g.edges.filter { case Edge(src, dst, prop) => prop == "advisor"}.count
      
      val advisorCount = g.triplets.filter(triplet => triplet.attr == "advisor").count
      println("Advisor count: " + advisorCount)

      val facts: RDD[String] = g.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
      facts.collect.foreach(println(_))  
  }
  
  private def runPageRankExample(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    // $example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "data/links.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.001).vertices
    
    val pages = sc.textFile("data/pages.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    
    // Join the ranks with the pages
    val ranksByPage = pages.join(ranks).map {
      case (id, (page, rank)) => (page, rank)
    }
    // Print the result
    println(ranksByPage.collect().sortBy(-_._2).mkString("\n"))
  }
  
  private def runTriangleCountingExample(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
   
    // Print the result
    println(triCountByUsername.collect.mkString("\n"))
  } 
  
  private def runConnectedComponentsExample(spark: SparkSession): Unit = {

    val sc = spark.sparkContext

    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }
}