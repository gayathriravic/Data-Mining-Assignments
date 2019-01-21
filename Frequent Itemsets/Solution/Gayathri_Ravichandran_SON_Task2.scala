import java.io.PrintWriter
import java.util
import java.io._

import scala.util.{Sorting, Try}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.{Row, SQLContext}
import spire.syntax.order

import scala.collection.mutable.{ListBuffer, Map => MMap}
//
//task1: use no paritions
//task2: use 2 or 3 paritions
//task3: use 15 partitions

object Gayathri_Ravichandran_SON_Task2{

  def main(args: Array[String]): Unit = {
    val spark_config = new SparkConf().setAppName("SON").setMaster("local")
    var spark_context = new SparkContext(spark_config);
    val sqlContext = new SQLContext(spark_context)
    val start_time = System.currentTimeMillis()
    //val path= "C:\\Users\\Gayathri\\Desktop\\Assignment_3\\inf553_assignment3\\Data\\yelp_reviews_test.txt"
    //val path ="C:\\Users\\Gayathri\\Documents\\yelp_reviews_large.txt"
    val path = args(0).toString
    val output_file = args(2).toString
    var inputFile = spark_context.textFile(path,2)
    val partitions = inputFile.getNumPartitions
    val inputFileArray = inputFile.map(x => x.split(","))
    //    val reviews = inputFileArray.map { line => line(1).toString }
    //   val singleFrequent = reviews.map { word => (word, 1) }.reduceByKey(_ + _) // single items and their counts. RDD[(word,count)]
    val basket = inputFileArray.map { line => (line(0), line(1)) }.groupByKey() // RDD[(review_id,List of words in review)]

    val temp1 = inputFile.map(line => line.split(",").map(elem => elem.trim))
    val temp2 = temp1.map(x => (x(0), x(1)))
    val baskets = temp2.groupByKey.mapValues(_.toSet)
    val basketCount = baskets.count().toInt // count the total number of baskets. --> 999
    val support = args(1).toInt
    val tempOutput = baskets.mapPartitions(x => { // for every partition.
      apriori(x,support/partitions).toIterator
    }).reduceByKey(_+_).collect()
    val outputCollected = tempOutput
    var counts = basket.mapPartitions( x=> {
      val tempList = ListBuffer.empty[(List[String])]
      x.toList.map(y => tempList += y._2.toList).iterator
      trueCounts(tempList,outputCollected).toIterator
    }).reduceByKey((l,r) => l+r)
    //
    val counts1 = counts.filter(a=>a._2.toDouble>=support).map(a=>a._1).sortBy(a=>a.size)

    var b = counts1.collect()
    def sort[A : Ordering](column : Seq[Iterable[A]]) = column.sorted
    val sorted_items = sort(b).sortBy(row => row.size)
    sorted_items.foreach(println)

    var writing = 1
    var final_output = Array.empty[String]
    val write_file = new PrintWriter(new File(output_file))

    sorted_items.foreach{
      item =>
        if(item.size != writing){
          writing += 1
          final_output.foreach(item=>
            if(item.equals(final_output.last)){
              write_file.write(item)
            }else{
              write_file.write(item + ", ")
            }
          )
          write_file.write("\n")
          write_file.write("\n")
          final_output = Array.empty[String]
          final_output = final_output :+ ("(" + item.mkString(",") + ")")
        }
        else{
          final_output = final_output :+ ("(" + item.mkString(",") + ")")
        }


    }

    if (final_output.nonEmpty){
      final_output.foreach( x =>
        if(x.equals(final_output.last)){
          write_file.write(x)
        }

        else{

          write_file.write(x + ", ")
        }
      )
    }
    write_file.close()


    val end_time = System.currentTimeMillis()
    println("Time taken in seconds: " + (end_time - start_time)/1000)
  }


  def trueCounts(x : ListBuffer[List[String]], output:Array[(Set[String],Integer)]): Array[(List[String],Int)]= {
    var a: Array[(List[String], Int)] = Array.empty
    var output1 = output.map(x=>(x._1.toList,x._2))
    a = output1.map(item1 => {
      var count = 0;
      x.map(item2 => {
        if (item1._1.forall(item2.contains)) {
          count += 1
        }
      })
      (item1._1, count)
    })
    a
  }


  def apriori(x : Iterator[(String, Set[String])],support: Double): Map[Set[String],Integer]={
    //    println("inside apriori")
    var basketlist = x.toList

    var basket1 = basketlist.map(a => a._2)

    var single = basket1.flatten.groupBy(identity).map(a=>(a._1,a._2.size))
    var single1 = single.filter(x=>x._2>=support).keySet


    var candidates : Map[Set[String], Integer] = Map.empty

    for(attr<-single1){
      candidates = candidates+ (Set(attr) ->1)
    }

    var threshold = 2
    var size = 2
    var init =candidates.size
    var items= single1.toList

    var resulttemp: Set[String] = Set.empty
    do{
      var comb = items.combinations(size)
      for (i <- comb){

        var c = 0
        for(j <- basket1){
          if (i.sorted.forall(j.contains)){
            c += 1
          }
        }
        if(c>=support){
          resulttemp = resulttemp ++ i.sorted
          candidates = candidates+(i.sorted.toSet -> 1)
        }
      }
      size += 1
      if(candidates.size <= init) {
        threshold = items.size + 1
      }

      else{
        threshold += 1
        items = resulttemp.toList
        resulttemp = Set.empty[String]
        init = candidates.size
      }
    }while(items.size >= threshold)
    candidates
  }
}




