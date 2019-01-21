import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object Task2 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark_config = new SparkConf().setAppName("Task2").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config)
    val sqlContext = new SQLContext(spark_context) // used for DataFrame / DataSet APIs
    import sqlContext.implicits._
    val inputfile = args(0)
    val outputfile = args(1)
    val df = sqlContext.read.format("csv").option("header", "true").load(inputfile)
    val df1 = df.orderBy(asc("Country"))
    val df2 = df1.filter("Salary != 'NA'")
    val df3= df2.filter("Salary != '0'")
    val country = df3.select("Country")

    val country1 = country.repartition(2)
    val standard = country1.rdd.mapPartitions(iter => Array(iter.size).iterator, true)

    val country2 = country.repartition(2,country("Country"))
    val partitions = country2.rdd.mapPartitions(iter => Array(iter.size).iterator, true)

    val standard_df = standard.toDF("Partitions")
    val list_standard  = standard_df.select("Partitions").rdd.map(r => r.get(0)).collect.toList

    val partitions_df = partitions.toDF("Partitions")
    val list_partitions = partitions_df.select("Partitions").rdd.map(r => r.get(0)).collect.toList

    /* start the time here */
    val t1 = System.currentTimeMillis()
    val country_counts = country1.rdd.map(word => (word.getString(0),1)).reduceByKey(_+_)
    val final_counts_dataframe = country_counts.toDF("Country","Salary").orderBy(asc("Country"))
    val t2 = System.currentTimeMillis()
    val time_taken1 = (t2-t1)
    //print("Time taken for task 1 --->" + time_taken1)
    /* End the time here for Task 1  */

    /*Start time for task 2 */

    val t3 = System.currentTimeMillis()
    //print("new time now is"+ t3)
    val country_counts1 = country2.rdd.map(word => (word.getString(0),1)).reduceByKey(_+_)
    val t4 = System.currentTimeMillis()
    val duration2 = (t4-t3)
    /* End time for task 2 */
    val time1 = Seq(time_taken1).toDF()
    val time2 = Seq(duration2).toDF()
    ///val final_standard1 =
    //partitions_df.show()
    //final_standard1.show()
    //print(list_standard)
    val list_standard1 = list_standard.map(_.toString).map(_.toInt)
    val list_partition1 = list_partitions.map(_.toString).map(_.toInt)
    val standard_first = list_standard1(0)
    val standard_second = list_standard1(1)
    val partition_first = list_partition1(0)
    val partition_second = list_partition1(1)

    val first_row = ("standard",standard_first,standard_second,time_taken1)
    val second_row = ("partition",partition_first,partition_second,duration2)

    val final_dataframe = spark_context.parallelize(List(first_row,second_row)).toDF()
    final_dataframe.repartition(2).write.format("csv").save(outputfile)

  }
}
