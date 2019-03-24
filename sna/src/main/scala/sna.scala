import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object sna {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Social Network Analysis")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Vertex RDD
    val vertexRDD = sc.parallelize(Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")), (3L, ("Stan Smith", "Person")),
      (4L, ("Homer Simpson", "Person")), (5L, ("Clark Kent", "Person")), (6L, ("James Smith", "Person"))))

    // Edge RDD
    val edgeRDD = sc.parallelize(Array(Edge(2L, 1L, "Friends"), Edge(2L, 5L, "Friends"), Edge(3L, 1L, "Friends"),
      Edge(3L, 4L, "Friends"), Edge(3L, 5L, "Friends"), Edge(3L, 6L, "Friends"),
      Edge(4L, 6L, "Friends"),
      Edge(5L, 1L, "Friends"), Edge(5L, 1L, "Friends"), Edge(5L, 6L, "Friends"),
      Edge(6L, 7L, "Friends")))

    // Default Vertex
    val defaultvertex = ("Self", "Missing")

    // Graph
    val facebook = Graph(vertexRDD, edgeRDD, defaultvertex)

    // Graph View
    println('\n'+"Graph View 1 - Using For Loop")
    for (triplet <- facebook.triplets.collect) {
      print(triplet.srcAttr._1)
      print(" is ")
      print(triplet.attr)
      print(" with ")
      println(triplet.dstAttr._1)
    }

    // One Liner
    // Graph View
    println('\n'+"Graph View 2 - Using One Liner")
    facebook.triplets.collect.foreach(triplet => println(triplet.srcAttr._1 + " is " + triplet.attr + " with " + triplet.dstAttr._1))

    // Removing Missing Vertex Id and its associated edges
    val facebook_step1 = facebook.subgraph(vpred = (id, prop) => prop._2 != "Missing")

    // Graph View
    println('\n'+"Graph View 3 - Removing Missing Vertex")
    for (triplet <- facebook_step1.triplets.collect) {
      print(triplet.srcAttr._1)
      print(" is ")
      print(triplet.attr)
      print(" with ")
      println(triplet.dstAttr._1)
    }

    // Removing Duplicate Edges
    val facebook_step2 = facebook_step1.partitionBy(PartitionStrategy.EdgePartition1D)
    val facebook_fixed = facebook_step2.groupEdges(merge = (edge1, edge2) => edge1)

    // Graph View
    println('\n'+"Graph View 4 - Removing Duplicate Edges")
    for (triplet <- facebook_fixed.triplets.collect) {
      print(triplet.srcAttr._1)
      print(" is ")
      print(triplet.attr)
      print(" with ")
      println(triplet.dstAttr._1)
    }

    // Graph Components
    println(s"Graph has ${facebook_fixed.numEdges} edges")
    println(s"Graph has ${facebook_fixed.numVertices} nodes")

    // Graph Property Operators - modify vertex property or edge property

    println('\n'+"Graph View 5 - Modifying Vertex Property")
    facebook
      .mapVertices((id,prop) => if(prop._1 == "Jacob Johnson") ("Janet Johnson","Person") else prop)
      .triplets.collect.foreach(triplet => println(triplet.srcAttr._1 + " is " + triplet.attr + " with " + triplet.dstAttr._1))

    println('\n'+"Graph View 5 - Modifying Edge Property")
    facebook
      .mapEdges(edge => if(edge.attr == "Friends") "Family" else edge.attr)
      .triplets.collect.foreach(triplet => println(triplet.srcAttr._1 + " is " + triplet.attr + " with " + triplet.dstAttr._1))

    // PageRank
    // Graph View
    println('\n'+"Graph View 6 - Pagerank")
    facebook_fixed.pageRank(0.1).vertices.collect.foreach(println)

    // Neighbourhood Aggregation - Another Implementation of .degrees method
    // Graph View
    println('\n'+"Graph View 7 - Neighbourhood Aggregation - Another Implementation of .degrees method")
    val aggregated_vertices = facebook_fixed.aggregateMessages[Int](triplet => (triplet.sendToSrc(1), triplet.sendToDst(1)), (a, b) => (a + b))
    for (aggregated_vertex <- aggregated_vertices.collect) {
      println(aggregated_vertex)

    }
  }
}
