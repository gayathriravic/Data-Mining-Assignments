import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector

// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$
object Task2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Task2").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inputFile = args(0).toString
    val algorithm = args(1).toString
    val clusterSize = args(2).toInt
    val iterations = args(3).toInt
    //
    //    "C:\\Users\\Gayathri\\Documents\\INF553_Assignment4\\INF553_Assignment4\\Data\\yelp_reviews_clustering_small.txt"
    val documents: RDD[Seq[String]] = sc.textFile(inputFile)
      .map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    val tfidf_parse: RDD[(Vector, Long)] = tfidf.zipWithIndex()
    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    case class tfidfObject(id: String, vec1: Vector, vec2: Vector)
    val data = sc.textFile(inputFile).zipWithIndex()

    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
    if(algorithm == "K") {
      val numClusters = clusterSize
      val numIterations = iterations
      val model = KMeans.train(tfidf, numClusters, numIterations, 1, "kmeans()", 42)
      var predicted_clusters = model.predict(tfidf).zipWithIndex().collect()
      val centers = model.clusterCenters

      // count per cluster.
      val count_map = collection.mutable.Map[Int, Int]()
      val document_cluster_hash = collection.mutable.Map[Int, Int]()

      for((cluster_id,doc_id)<- predicted_clusters){
        document_cluster_hash.put(doc_id.toInt,cluster_id)
        if (count_map.contains(cluster_id)){
          val count = count_map.getOrElse(cluster_id,0)
          count_map.put(cluster_id.toInt,count+1)
        }
        else {
          count_map.put(cluster_id.toInt,1)
        }
      }
      // error per cluster.

      //document_cluster_hash.foreach(println)
      val error_per_cluster = collection.mutable.Map[Int, Double]()
      var WSSE1 = 0.0
      for (k <- 0 until numClusters) {
        var error = 0.00
        for ((k1, v1) <- document_cluster_hash) {
          if (v1 == k) {
            tfidf_parse.collect().foreach { line =>
              if (line._2.toInt == k1) {
                error += Vectors.sqdist(line._1, model.clusterCenters(k))
              }
            }
          }
        }

        error_per_cluster.put(k, Math.sqrt(error))
        WSSE1 = WSSE1 + error_per_cluster(k) * error_per_cluster(k)
      }

      var words_per_cluster: collection.mutable.Map[Int, List[String]] = collection.mutable.Map.empty
      val tfidf_rev = data.map { pair => pair.swap }.collect()
      for (line <- document_cluster_hash) {
        if (words_per_cluster.contains(line._2)) {
          words_per_cluster(line._2) = tfidf_rev(line._1)._2.toString.split(" ").toList ::: words_per_cluster(line._2)
        }
        else {
          words_per_cluster.put(line._2, tfidf_rev(line._1)._2.toString.split(" ").toList)
        }
      }

      var top_10_words: collection.mutable.Map[Int, Array[String]] = collection.mutable.Map.empty
      for (line <- words_per_cluster.toSeq.sortBy(_._1)) {
        var x = sc.parallelize(line._2).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2).take(10).map { case (a, b) => "\"" + a + "\"" }
        top_10_words.put(line._1, x)
      }
      top_10_words.foreach(println)

      val file = new File("Gayathri_Ravichandran_Cluster_small_K_8_20.json")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("{ \n")
      bw.write("\t \"algorithm\": \"K-Means\", \n")
      bw.write("\t \"WSSE\": " + WSSE1 + ", \n")
      bw.write("\t \"Clusters\": [ { \n")
      for (i <- 0 to numClusters-1) {
        if (i != 0) {
          bw.write("\t { \n")
        }
        bw.write("\t \t \"id\":  " + (i+1) + ", \n")
        bw.write("\t \t \"size\": " + count_map(i) + ", \n")
        bw.write("\t \t \"error\": " + error_per_cluster(i) + ",\n")
        bw.write("\t \t \"terms\": [" + top_10_words(i).mkString(", ") + "]  \n")
        if (i == numClusters-1) {
          bw.write("\t } ] \n")
        }
        else
          bw.write("\t }, \n")
      }
      bw.write("} \n")
      bw.close()
    }

    else{
      // Bisecting K-Means
      println("BISECTING K-MEANS")
      val numClusters = clusterSize
      val numIterations = iterations
      val bkm = new BisectingKMeans().setK(numClusters).setMaxIterations(numIterations).setSeed(42)
      val model = bkm.run(tfidf)
      val WSSSE = model.computeCost(tfidf)
      println(s"Within Set Sum of Squared Errors = $WSSSE")
      var predicted_clusters = model.predict(tfidf).zipWithIndex().collect()
      val centers = model.clusterCenters

      // count per cluster.
      val count_map = collection.mutable.Map[Int, Int]()
      val document_cluster_hash = collection.mutable.Map[Int, Int]()

      for((cluster_id,doc_id)<- predicted_clusters){
        document_cluster_hash.put(doc_id.toInt,cluster_id)
        if (count_map.contains(cluster_id)){
          val count = count_map.getOrElse(cluster_id,0)
          count_map.put(cluster_id.toInt,count+1)
        }
        else {
          count_map.put(cluster_id.toInt,1)
        }
      }
      // error per cluster.

      //document_cluster_hash.foreach(println)
      val error_per_cluster = collection.mutable.Map[Int, Double]()
      var WSSE1 = 0.0
      for (k <- 0 until numClusters) {
        var error = 0.00
        for ((k1, v1) <- document_cluster_hash) {
          if (v1 == k) {
            tfidf_parse.collect().foreach { line =>
              if (line._2.toInt == k1) {
                error += Vectors.sqdist(line._1, model.clusterCenters(k))
              }
            }
          }
        }

        error_per_cluster.put(k, Math.sqrt(error))
        WSSE1 = WSSE1 + error_per_cluster(k) * error_per_cluster(k)
      }

      var words_per_cluster: collection.mutable.Map[Int, List[String]] = collection.mutable.Map.empty
      val tfidf_rev = data.map { pair => pair.swap }.collect()
      for (line <- document_cluster_hash) {
        if (words_per_cluster.contains(line._2)) {
          words_per_cluster(line._2) = tfidf_rev(line._1)._2.toString.split(" ").toList ::: words_per_cluster(line._2)
        }
        else {
          words_per_cluster.put(line._2, tfidf_rev(line._1)._2.toString.split(" ").toList)
        }
      }

      var top_10_words: collection.mutable.Map[Int, Array[String]] = collection.mutable.Map.empty
      for (line <- words_per_cluster.toSeq.sortBy(_._1)) {
        var x = sc.parallelize(line._2).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2).take(10).map { case (a, b) => "\"" + a + "\"" }
        top_10_words.put(line._1, x)
      }
      top_10_words.foreach(println)

      val file = new File("Gayathri_Ravichandran_Cluster_small_B_8_20.json")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("{ \n")
      bw.write("\t \"algorithm\": \"Bisecting K-Means\", \n")
      bw.write("\t \"WSSE\": " + WSSE1 + ", \n")
      bw.write("\t \"Clusters\": [ { \n")
      for (i <- 0 to numClusters-1) {
        if (i != 0) {
          bw.write("\t { \n")
        }
        bw.write("\t \t \"id\":  " + (i+1) + ", \n")
        bw.write("\t \t \"size\": " + count_map(i) + ", \n")
        bw.write("\t \t \"error\": " + error_per_cluster(i) + ",\n")
        bw.write("\t \t \"terms\": [" + top_10_words(i).mkString(", ") + "]  \n")
        if (i == numClusters-1) {
          bw.write("\t } ] \n")
        }
        else
          bw.write("\t }, \n")
      }
      bw.write("} \n")
      bw.close()
    }
  }
}
// scalastyle:on println
