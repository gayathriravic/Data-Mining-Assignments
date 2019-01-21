import breeze.linalg.mapValues
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark
import org.apache.spark.sql.{Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.round


/*
Have to do this.
 */
object Task3 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark_config = new SparkConf().setAppName("Task3").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config)
    val sqlContext = new SQLContext(spark_context) // used for DataFrame / DataSet APIs
    import sqlContext.implicits._

    // Read the file.
    val input_file = args(0)
    val output_file = args(1)
    val df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_file)
    //df.printSchema()
    val df11 = df.withColumn("Salary",regexp_replace(df("Salary"),",",""))
    // define the variables.
    val df1 = df11.orderBy(asc("Country"))
    val df2 = df1.filter("Salary != 'NA'")
    val df3= df2.filter("Salary != '0'")
    // filter out only yearly salaries and also convert NA to Yearly.
    val df33 = df3.withColumn("SalaryType", regexp_replace(df3("SalaryType"),"NA","Yearly"))
    //val df34 = df33.withColumn("SalaryType", when($"SalaryType" === "Monthly" , ($"Salary"/12.0))).show(10)
    val df34 = df33.filter("SalaryType == 'Yearly'")
    val country1 = df3.select("Country") // this is for total computation.
    val country = country1.orderBy(asc("Country"))
    // convert the salaries to integer to find min and max.
    //val df331 = df33.filter("Salary != 0.00000000")
    val df31 = df3.filter("Salary!=0")
    val df4 = df31.withColumn("Salary",df31("Salary").cast(IntegerType))
    val to_find_max_and_min = df4.select("Country","Salary") // to find the max and min

    //print("MONTHLYYY")
    //val df34 = df4.withColumn("Salary", when($"SalaryType" === "Monthly", "$Salary/12.0"))
    //df34.show(10)

    // convert the salaries to Double to find average salaries.
    val df6=df33.withColumn("Salary",df33("Salary").cast(DoubleType))
    val df7 = df6.withColumn("Salary",when($"SalaryType" ==="Monthly",$"Salary"*12.0).otherwise($"Salary"))
    val df8 = df7.withColumn("Salary",when($"SalaryType" === "Weekly", $"Salary"*52.0).otherwise($"Salary"))
    //val df8 = df7.withColumn("Salary",when($"SalaryType" === "Weekly", $"Salary"/52.14).otherwise($"Salary"))
    val for_min_max = df4.withColumn("Salary",when($"SalaryType" ==="Monthly",$"Salary"*12).otherwise($"Salary"))
    val for_min_max_final  = for_min_max.withColumn("Salary",when($"SalaryType" ==="Weekly",$"Salary"*52).otherwise($"Salary"))
    //df8.show(10)
    val average_compute = df8.select("Country","Salary")

    // STEP 1 --> DO NOT TOUCH THIS CODE
    //we have the total now. we must now calculate the (key,value) pairs for the dataframe.
    val country_counts_dataframe = country.rdd.map(word => (word.getString(0),1)).reduceByKey(_+_)
    //print("*** ===== Country counts ===== ****")
    val df_country = country_counts_dataframe.toDF("Country","Salary").orderBy(asc("Country"))
    df_country.show(10)

    // STEP 2 --> FIND MAX AND MIN
    //print("MIN AND MAX")
    val find_max_dataframe =for_min_max_final.groupBy("Country").max("Salary")
    val find_min_dataframe = for_min_max_final.groupBy("Country").min("Salary")
    find_min_dataframe.show()
    find_max_dataframe.show()

    // STEP 3 --> FIND THE AVERAGE
    //val avg = average_compute.cole
    // lect.map(_.toSeq)
    //println("AVERAGEE")
    //val updatedDf = average_compute.withColumn("Salary", regexp_replace(col("name"), ",", ""))
    //print("UPDATEDDDD")
    //updatedDf.show(10)
    val avg = average_compute.rdd.map(x => (x.get(0).toString, x.get(1).asInstanceOf[Double])).collect()
    var data = spark_context.parallelize((avg))
    val mapped = data.mapValues(mark => (mark, 1))
    val reduced = mapped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val average = reduced.map{ x=> val temp=x._2 ; val total=temp._1; val count = temp._2; (x._1,total/count)}
    val final_avg = average.collect()
    val final_average_dataframe = spark_context.parallelize(final_avg).toDF("Country","AvgSalary").orderBy(asc("Country"))
    val y = final_average_dataframe.withColumn("AvgSalary", round($"AvgSalary", 2))
    //print("ROUNDEDD OFFFF")
    //y.show(10)
    val df_res = df_country.join(find_min_dataframe,"Country").orderBy(asc("Country"))
    val df_res2 = df_res.join(find_max_dataframe,"Country").orderBy(asc("Country"))
    val df_res3 = df_res2.join(y,"Country").orderBy(asc("Country"))
    df_res3.show(10)
    //final_average_dataframe.show(10)
    df_res3.repartition(1).write.format("csv").save(output_file)
  }
}