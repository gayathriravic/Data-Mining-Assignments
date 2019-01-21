import java.io.PrintWriter
import java.util
import java.io._


import scala.util.Try
import org.apache.spark.{SparkConf, SparkContext, rdd}
//import org.apache.spark
//import org.apache.spark.ml.feature.StringIndexer
//import org.apache.spark.ml.recommendation
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map => MMap}

object ModelBasedCF{

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val spark_config = new SparkConf().setAppName("ModelBasedCF").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config);
    val sqlContext = new SQLContext(spark_context)
    import sqlContext.implicits._
    // training set
    val input_file = args(0)
    val output_file = args(1)
    val data = spark_context.textFile(input_file)
    val full_rdd = data.mapPartitionsWithIndex { (i, j) => if (i == 0) j.drop(1) else j}
    //testing set
    val test_data= spark_context.textFile(output_file);
    println(test_data.count())
    println("'countone")
    val full_test_rdd = test_data.mapPartitionsWithIndex { (i, j) => if (i == 0) j.drop(1) else j}
    val rddAsArray = full_rdd.map(x => x.split(","))
    val testrddAsArray = full_test_rdd.map(x => x.split(","))
    print("reading test data")


    var usertoIntHash = mutable.HashMap.empty[String,Int]
    var businesstoIntHash = mutable.HashMap.empty[String,Int]
    var reverseDict1 = mutable.HashMap.empty[Int,String]
    var reverseDict2 = mutable.HashMap.empty[Int,String]
    var index1 = 0
    var index2 = 0
    full_rdd.collect().foreach(a=>{
      val Array(x, y,z, _*) = a.split(",")
      val user_id = x
      val business_id = y
      val ratings = z
      if (!usertoIntHash.contains(user_id)){
        usertoIntHash(user_id) = index1
        reverseDict1(index1) = user_id
        var value = index1
        index1 += 1
      }
      //        else{
      //          var value = usertoIntHash.getOrElse(user_id,0)
      //        }

      if (!businesstoIntHash.contains(business_id)) {
        businesstoIntHash(business_id) = index2
        reverseDict2(index2) = business_id
        var value = index2
        index2 += 1
      }
    })

    full_test_rdd.collect().foreach(a=>{
      val Array(x, y,z, _*) = a.split(",")
      val user_id = x
      val business_id = y
      val ratings = z
      if (!usertoIntHash.contains(user_id)){
        // yes. that's right.
        usertoIntHash(user_id) = index1
        reverseDict1(index1) = user_id
        var value = index1
        index1 += 1
      }

      if (!businesstoIntHash.contains(business_id)){
        businesstoIntHash(business_id) = index2
        reverseDict2(index2) = business_id
        var value = index2
        index2 += 1
      }

    })

    var testRDD = testrddAsArray.map(line=>Seq(usertoIntHash(line(0).toString),businesstoIntHash(line(1).toString), line(2).toString.toDouble))
    var trainRDD = rddAsArray.map(line=>Seq(usertoIntHash(line(0).toString),businesstoIntHash(line(1).toString), line(2).toString.toDouble))

    var otherTest = testrddAsArray.map(line=>((usertoIntHash(line(0).toString),businesstoIntHash(line(1).toString)), line(2).toString.toDouble))
    println(testRDD.count())
    println("TEST RDD COUNT")


    // Training Data.
    val ratings_train = trainRDD.map { case Seq(user,item,rate) => Rating(user.asInstanceOf[Int],item.asInstanceOf[Int],rate.asInstanceOf[Float])}
    val ratings_test = testRDD.map { case Seq(user,item,rate) => Rating(user.asInstanceOf[Int],item.asInstanceOf[Int],rate.asInstanceOf[Float])}

    println("ratings test count")
    //    ratings_test.foreach(println)
    println(ratings_test.count())
    println("done ratings test")
    // Set optimal parameters.  --- 10,20,0.09
    val rank = 20
    val numIterations = 20
    val model = ALS.train(ratings_train, rank, numIterations, 0.3)

    // Evaluate the model on training data.
    val ratingsRDD = ratings_test.map { case Rating(user, product, rate) =>
      (user, product)
    }

    println("rating rdd count")
    println(ratingsRDD.count())
    println("done ratings rdd")
    //    val predictions1 = model.predict(ratingsRDD)
    //    println("predictions one count")
    //    println(predictions1.count())
    //    println("done predictions one count")
    val predictions =
    model.predict(ratingsRDD).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    println("predictiouns count")
    println(predictions.count())
    val ratesAndPreds = ratings_test.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val otherTest1 = predictions.collect().toMap
    val remaining = otherTest.filter(elem => !otherTest1.keySet.contains(elem._1))
    val ans1 = remaining.map{case((x,y),z) => (reverseDict1(x),reverseDict2(y),z)}
    val ans = predictions.map{case((x,y),z) => (reverseDict1(x),reverseDict2(y),z)}
    val finalans= ans
    val final_output =finalans.sortBy(r => (r._1, r._2)).collect.toList
    val temp1 = finalans.map { case (user,item,rate) => Rating(user.asInstanceOf[Int],item.asInstanceOf[Int],rate.asInstanceOf[Float])}

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    val end = System.currentTimeMillis()
    val TimeTaken = (end - start) / 1000

    val RSME = math.sqrt(MSE)
    println(s"Mean Squared Error = $RSME")

    val pw = new PrintWriter(new File("Gayathri_Ravichandran_ModelBasedCF.txt"))
    //pw.println("user_id,business_id,stars")
    for(line <- final_output){
      pw.write(line._1+","+line._2+","+line._3+"\n")
    }


    val temp3= otherTest.join(predictions)
    println("temp3 count")
    println(temp3.count())
    val counts = otherTest.join(predictions).map { case ((user, item), (given, ours)) => ((user,item),Math.abs(given-ours))}
    val c0 = counts.filter(row => row._2 >=0 && row._2 <1).count()
    val c1 = counts.filter(row => row._2 >=1 && row._2 <2).count()
    val c2 = counts.filter(row => row._2 >=2 && row._2 <3).count()
    val c3 = counts.filter(row => row._2 >=3 && row._2 <4).count()
    val c4 = counts.filter(row => row._2 >=4).count()

    pw.write(">=0 and <1: "+c0+"\n")
    pw.write(">=1 and <2: "+c1+"\n")
    pw.write(">=2 and <3: "+c2+"\n")
    pw.write(">=3 and <4: "+c3+"\n")
    pw.write(">=4: "+c4+"\n")


    //  Finding the counts.

    pw.write("RMSE:" + RSME+"\n")
    pw.write("Time: "+ TimeTaken+"\n")

    pw.close()
  }
}



