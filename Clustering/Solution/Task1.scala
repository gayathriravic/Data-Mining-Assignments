import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}


object Task1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Task1").setMaster("local")
    val sc = new SparkContext(conf)
    val inputFile = args(0).toString
    val feature = args(1).toString
    val documents: RDD[Seq[String]] = sc.textFile(inputFile)
      .map(_.split(" ").toSeq)
    val data = sc.textFile(inputFile).zipWithIndex()
    val hashingTF = new HashingTF(15000)

    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    var tfidf_parse: RDD[(Vector, Long)] = tfidf.zipWithIndex() // This is of the form -->((1048576,[reviews],id))
    if (feature == "T") {
      println("tf-idf")
      val tfidf_parse1: RDD[(Vector, Long)] = tfidf.zipWithIndex()
      tfidf_parse = tfidf_parse1

    }
    else {
      println("word count")
      val tfidf_parse2: RDD[(Vector, Long)] = tf.zipWithIndex()
      tfidf_parse = tfidf_parse2
    }

    var file = new File("Gayathri_Ravichandran_KMeans_small_W_5_20.json")

    if(feature == "W"){
      file = new File("Gayathri_Ravichandran_KMeans_small_W_5_20.json")}
    else{
      file = new File("Gayathri_Ravichandran_KMeans_small_T_5_20.json")
    }

    println("FILE IS"+ file)


    val K = args(2).toInt
    val seed = 20181031
    val iterations = args(3).toInt


    val document_count = tfidf.count.toInt
    var document_cluster_hash = collection.mutable.Map[Int, Int]()
    var cluster_centroid_hash = collection.mutable.Map[Int, Vector]()
    var rest_document_cluster_hash = collection.mutable.Map[Int, Int]()
    var rest_cluster_centroid_hash = collection.mutable.Map[Int, Vector]()
    val initial_centroids = new scala.util.Random(seed)

    for (doc_id <- 1 to K) {
      var document = initial_centroids.nextInt(document_count)
      document_cluster_hash.put(document, doc_id)
      rest_document_cluster_hash.put(document, doc_id)
      tfidf_parse.collect().foreach {
        row =>
          if (row._2.toInt == document) {
            cluster_centroid_hash.put(doc_id, row._1)
            rest_cluster_centroid_hash.put(doc_id, row._1)
          }
      }
    }

    val cluster_doc = collection.mutable.Map[Int, RDD[Vector]]()
    for (cluster_id <- 1 to K) {
      var a = tfidf_parse.collect()
      a.foreach {
        row =>
          var minimum_distance = Double.MaxValue
          var final_cluster = 1
          var distance = Math.sqrt(Vectors.sqdist(row._1, cluster_centroid_hash(cluster_id)))
          if (minimum_distance > distance) {
            minimum_distance = distance;
            final_cluster = cluster_id;
          }
          if (!document_cluster_hash.contains(row._2.toInt))
            document_cluster_hash.put(row._2.toInt, final_cluster)
          else
            document_cluster_hash.put(row._2.toInt, final_cluster)
      }
      for (c <- 1 to K) {

        var tempList: Array[Double] = Array.emptyDoubleArray
        for ((key, value) <- document_cluster_hash) {
          if (value == c) {
            var t = tfidf_parse.collect()
            t.foreach { line =>
              if (line._2 == key) {
                tempList :+ line._1
              }
            }
          }
        }
        val vec = Vectors.dense(tempList)
        if (vec.size > 0) {
          cluster_doc.put(c, sc.parallelize(Seq(vec)))
        }
      }

      for ((key, value) <- cluster_doc) {
        val summary: MultivariateStatisticalSummary = Statistics.colStats(value)
        cluster_centroid_hash(key) = summary.mean
      }
    }


    var iter = 0
    while (iter < iterations) {
      val cluster_doc = collection.mutable.Map[Int, RDD[Vector]]()
      var a = tfidf_parse.collect()
      a.foreach {
        row =>
          var minimum_distance = Double.MaxValue
          var final_cluster = 1
          for (cluster_id <- 1 to K) {
            var distance = Math.sqrt(Vectors.sqdist(row._1, cluster_centroid_hash(cluster_id)))
            if (minimum_distance > distance) {
              minimum_distance = distance;
              final_cluster = cluster_id;
            }
          }

          if (!document_cluster_hash.contains(row._2.toInt))
            document_cluster_hash.put(row._2.toInt, final_cluster)
          else
            document_cluster_hash.put(row._2.toInt, final_cluster)
      }

      for (c <- 1 to K) {
        var tempList: Array[Double] = Array.emptyDoubleArray
        for ((key, value) <- document_cluster_hash) {
          if (value == c) {
            var t = tfidf_parse.collect()
            t.foreach { line =>
              if (line._2 == key) {
                tempList :+ line._1
              }
            }
          }
        }

        val vec = Vectors.dense(tempList)
        if (vec.size > 0) {
          cluster_doc.put(c, sc.parallelize(Seq(vec)))
        }
      }

      for ((key, value) <- cluster_doc) {
        val summary: MultivariateStatisticalSummary = Statistics.colStats(value)
        cluster_centroid_hash(key) = summary.mean
      }
      iter += 1
    }
    println("size.")
    println(document_cluster_hash.size)


    var error_per_cluster: collection.mutable.Map[Int, Double] = collection.mutable.Map.empty
    var WSSE = 0.0
    for ((k, v) <- cluster_centroid_hash) {
      var error = 0.00
      for ((k1, v1) <- document_cluster_hash) {
        if (k == v1) {
          tfidf_parse.collect().foreach { line =>
            if (line._2.toInt == k1) {
              error += Vectors.sqdist(line._1, v)
            }
          }
        }
      }
      error_per_cluster.put(k, Math.sqrt(error))
      WSSE = WSSE + error_per_cluster(k) * error_per_cluster(k)
    }
    println(WSSE)


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
      var x = sc.parallelize(line._2).map(word => (word, 1)).reduceByKey(_ + _).sortBy(-_._2).take(10).map { case (a, b) => "\"" + a + "\"" }
      top_10_words.put(line._1, x)
    }
    top_10_words.foreach(println)

    val count_map = collection.mutable.Map[Int, Int]()

    for (i <- 0 until document_count) {
      for (k <- 1 to K) {
        if (document_cluster_hash(i) == k) {
          if (!count_map.contains(k))
            count_map.put(k, 1)
          else
            count_map(k) += 1
        }
      }
    }


    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("{ \n")
    bw.write("\t \"algorithm\": \"K-Means\", \n")
    bw.write("\t \"WSSE\": " + WSSE + ", \n")
    bw.write("\t \"Clusters\": [ { \n")
    for (i <- 1 to K) {
      if (i != 1) {
        bw.write("\t { \n")
      }
      bw.write("\t \t \"id\":  " + i + ", \n")
      bw.write("\t \t \"size\": " + count_map(i) + ", \n")
      bw.write("\t \t \"error\": " + error_per_cluster(i) + ",\n")
      bw.write("\t \t \"terms\": [" + top_10_words(i).mkString(", ") + "]  \n")
      if (i == K) {
        bw.write("\t } ] \n")
      }
      else
        bw.write("\t }, \n")
    }
    bw.write("} \n")
    bw.close()
  }
}


