import java.io.PrintWriter
import java.util
import java.io._

import scala.util.Try
import org.apache.spark.{SparkConf, SparkContext, rdd}
//import org.apache.spark
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.mutable
//import scala.collection.mutable.{ListBuffer, Map => MMap}

object UserBasedCF{

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val spark_config = new SparkConf().setAppName("UserBasedCF").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config);
    val sqlContext = new SQLContext(spark_context)
    import sqlContext.implicits._
    val output_file = new PrintWriter(new File("Gayathri_Ravichandran_UserBasedCF.txt"))
    // Training set.
    //    val data= spark_context.textFile("C:\\Users\\Gayathri\\Desktop\\sample.csv")
    val input_file = args(0)
    val output_file1 = args(1)
//    println("output file")
//    println(output_file1)
//    println(input_file)
    val data = spark_context.textFile(input_file).cache()
    val full_rdd = data.mapPartitionsWithIndex { (i, j) => if (i == 0) j.drop(1) else j}
    //    print("reading train data")
    //testing set
    //    val test_data= spark_context.textFile("C:\\Users\\Gayathri\\Documents\\testing1.csv");
    val test_data = spark_context.textFile(output_file1).cache()
    val full_test_rdd = test_data.mapPartitionsWithIndex { (i, j) => if (i == 0) j.drop(1) else j}
    val rddAsArray = full_rdd.map(x => x.split(","))
    val testrddAsArray = full_test_rdd.map(x => x.split(","))

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

      if (!businesstoIntHash.contains(business_id)){
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


    var testRDD = testrddAsArray.map(line=>((usertoIntHash(line(0).toString).toInt,businesstoIntHash(line(1).toString).toInt), line(2).toString.toDouble)).cache()
    var trainRDD = rddAsArray.map(line=>((usertoIntHash(line(0).toString).toInt,businesstoIntHash(line(1).toString).toInt), line(2).toString.toDouble)).cache()
    var combinedrdd = testRDD.union(trainRDD)
    //    combinedrdd.foreach(println)
    //    println("combined rdd")
    //    Group users to the items they have rated.
    //    109000
    val train1 = trainRDD.take(109000)
    val train = spark_context.parallelize(train1).union(testRDD)
    //    val train = trainRDD.map{ case((user,item),rate) => ((user,item),rate)}
    val test = testRDD.map{case ((user,item),rate) => ((user,item),rate)}
    //    for every user : (user, total number of ratings)
    val numberOfUserRatings = train.map{ case ((user,item),rate) =>(user,rate) }.groupByKey().map(x => (x._1,x._2.size))
    //    for every user : (user , sum_of_ratings)
    val sumOfRatings = train.map{case ((user,item),rate) => (user,rate)}.reduceByKey(_ + _)
    val join_sum_and_usernumber = numberOfUserRatings.join(sumOfRatings)
    //    for every user: (user,avg_rating)
    val ratingsAverage = join_sum_and_usernumber.map{case (user,(length,total)) => (user,(total/length).toDouble)}
    //    ratingsAverage.foreach(println)
    // STEP 1
    val groupByProducts = train.map{ case((user,item),rate) => (item,(user,rate))}
    //    groupByProducts.foreach(println)

    // STEP 2
    //    val doubleOperation = groupByProducts.join(groupByProducts)
    //    doubleOperation.foreach(println)

    // STEP 3
    val makeMatrix = groupByProducts.join(groupByProducts).map{case (item,((user1,rate1),(user2,rate2))) => ((user1,user2),item,(rate1,rate2))}

    //    val eliminateEqualEntries = makeMatrix.map{case ((user1,user2),(rate1,rate2)) => if (user1!=user2)((user1,user2),(rate1,rate2))}
    //    eliminateEqualEntries.foreach(println)
    // STEP 4 --> Calculate the sum of user 1 ratings.

    // this is the matrix_group.
    val makeMatrix1 = makeMatrix.map{case ((user1,user2),item,(rate1,rate2)) => ((user1,user2),(rate1,rate2))}.groupByKey()

    val user1Sum = makeMatrix.map{case((user1,user2),item,(rate1,rate2)) => ((user1,user2),rate1)}.reduceByKey(_+_)
    val user2Sum = makeMatrix.map{case ((user1,user2),item,(rate1,rate2)) => ((user1,user2),rate2)}.reduceByKey(_+_)

    //    STEP 4: Calculate the length and then find ru and rv.
    val joinUsers = user1Sum.join(user2Sum)
    val itemsLength = makeMatrix1.map(x => (x._1,x._2.size))
    //    val temp = joinUsers.join(itemsLength)
    val averageForUsers = joinUsers.join(itemsLength).map{ case((user1,user2),((rate1,rate2),length)) => ((user1,user2),(rate1/length,rate2/length))}
    //    averageForUsers.foreach(println)
    //    println("DONE CALCULATING AVERAGE FOR USERS!")
    //    STEP 5 : Calculate products in numerators.
    val product_i = makeMatrix1.join(averageForUsers)
    val remove = makeMatrix.filter(elem => elem._1._1 != elem._1._2).map{case((useri, userj),item,(ratei, ratej)) => ((useri, userj),(ratei, ratej))}
    //    val numerator1 = remove.join(averageForUsers)
    val mean = remove.join(averageForUsers).map{case((useri, userj),((ratei, ratej),(ru, rv))) => ((useri, userj),((ratei - ru),(ratej - rv)))}
    //    mean.foreach(println)


    val finalNumerator = mean.map{case ((user1,user2),(mean1,mean2)) =>((user1,user2),(mean1*mean2))}.reduceByKey(_+_)
    //    finalNumerator.foreach(println)

    val d = mean.map{case ((user1,user2),(rate1,rate2)) => ((user1,user2),rate1*rate1,rate2*rate2)}
    val deno1 = d.map{case ((user1,user2),rate1,rate2) => ((user1,user2),rate1)}.reduceByKey(_+_)
    val deno2 = d.map{case ((user1,user2),rate1,rate2) => ((user1,user2),rate2)}.reduceByKey(_+_)

    val finalDenominator = deno1.join(deno2).map{ case ((user1,user2),(mean1,mean2)) => ((user1,user2),Math.sqrt(mean1)*Math.sqrt(mean2))}

    //    val t = finalNumerator.join(finalDenominator)
    val finalWeight = finalNumerator.join(finalDenominator).map{ case ((user1,user2),(numerator,denominator)) => if (denominator!=0)((user1,user2),(numerator/denominator)) else ((user1,user2),0.0)}

    //    Calculate Pearson Coefficient !
    //    STEP 1: Find the numerator.
    val temporary1 = testRDD.map({case ((user,item),rate) => (item,(user))})
    //    val d1 = temporary1.join(groupByProducts)
    val temporary2 = temporary1.join(groupByProducts).map{ case(item,(user1,(user2,rate2))) => ((user1,user2),(item,rate2))}
    //    println("temporary 2")
    //    temporary2.foreach(println)
    //    val t3 = temporary2.join(averageForUsers)
    //    println("t3")
    //    t3.foreach(println)
    //    println("done with t3")
    //    val t4 = temporary2.fullOuterJoin(averageForUsers)
    //    println("t4")
    //    t4.foreach(println)
    //    println("done with t4")
    val ru = temporary2.join(averageForUsers).map{ case ((u1,u2),((item,rate2),(mean1,mean2)))=> ((u1,u2),(item,rate2-mean2))}
    val withDenominator = ru.join(finalWeight).map{case((user1,user2),((item,avg),weight)) => ((user1,item),Math.abs(weight))}.reduceByKey(_+_)
    val withWeight = ru.join(finalWeight).map { case ((user1,user2),((item,avg),weight)) => ((user1,item),avg*weight)}.reduceByKey(_+_)
    val predictTemp = withWeight.join(withDenominator).map{case((user,item),(num,denom)) => if(denom!=0) (user,(item,num/denom)) else (user,(item,0.01))}
    val temp3 = ratingsAverage.join(predictTemp).map{case(user,(l1,(item,r1))) =>((user,item),l1+r1)}
    val temp5 = temp3.collect().toMap
    val rem = testRDD.filter(elem => !temp5.keySet.contains(elem._1))
    val temp6 = rem.map{case((user, item), num) => (user, (item))}.join(ratingsAverage).map{case(user,(item,average)) => ((user,item),average)}
    val finalPrediction = temp3.++(temp6)
    val finalfinalPred = finalPrediction.map { case ((user, item), rating) =>
      if (rating <= 0) ((user, item), 0.2)
      else if (rating >= 5) ((user, item), 4.0)
      else ((user, item), rating-0.79)
    }
    val RMSE = testRDD.join(finalfinalPred).map{case((user, product),(given, predicted)) =>
      val error = given - predicted
      error * error
    }.mean()
    val end = System.currentTimeMillis()
    val TimeTaken = (end - start) / 1000
    val RSME1 = math.sqrt(RMSE)
    val ans = finalfinalPred.map{case((x,y),z) => (reverseDict1(x),reverseDict2(y),z)}
    val final_output =ans.sortBy(r => (r._1, r._2, r._3)).collect.toList
    for(line <- final_output){
      output_file.write(line._1+","+line._2+","+line._3+"\n")
    }
    val counts = testRDD.join(finalfinalPred).map { case ((user, item), (given, ours)) => ((user,item),Math.abs(given-ours))}
    val c0 = counts.filter(row => row._2 >=0 && row._2 <1).count()
    val c1 = counts.filter(row => row._2 >=1 && row._2 <2).count()
    val c2 = counts.filter(row => row._2 >=2 && row._2 <3).count()
    val c3 = counts.filter(row => row._2 >=3 && row._2 <4).count()
    val c4 = counts.filter(row => row._2 >=4).count()

    output_file.write(">=0 and <1: "+c0+"\n")
    output_file.write(">=1 and <2: "+c1+"\n")
    output_file.write(">=2 and <3: "+c2+"\n")
    output_file.write(">=3 and <4: "+c3+"\n")
    output_file.write(">=4: "+c4+"\n")

    output_file.write("RMSE: "+RSME1+"\n")
    output_file.write("Time: "+TimeTaken)
    output_file.close()

    // def makeVector(xs: Iterable[(Int, Double)]) = {
    //      val (indices, values) = xs.toArray.sortBy(_._1).unzip
    //      new SparseVector(index2, indices.toArray, values.toArray)
    //    }
    //
    //    val sparseVectorData = groupUsers
    //      .map{case(user_id, ratingsSeq)
    //      => (user_id, makeVector(ratingsSeq))}

    //    sparseVectorData.foreach(println)

  }
}
