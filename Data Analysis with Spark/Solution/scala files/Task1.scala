import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
object Task1 {
  //C:\Users\Gayathri\Desktop\jdk-8u181-windows-x64.exe
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark_config = new SparkConf().setAppName("Task1").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config)
    val sqlContext = new SQLContext(spark_context) // used for DataFrame / DataSet APIs
    //println("Country", "Salary", "Salary Type")
    val input_file = args(0)
    val output_file = args(1)
    val df = sqlContext.read.format("csv").option("header", "true").load(input_file)
    //df.printSchema()

    val df1 = df.orderBy(asc("Country"))
    //print("yesss")
    val df2 = df1.filter("Salary != 'NA'")
    val df3= df2.filter("Salary != '0'")
    val country = df3.select("Country")
    //val salary = df1.select("Salary")
    //val salary_type = df1.select("SalaryType")
    //println(country.count())
    val country1 = country.repartition(2)
    val country_counts = country1.rdd.map(word => (word.getString(0),1)).reduceByKey(_+_)
    val number = country_counts.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    import sqlContext.implicits._
    val final_counts_dataframe = country_counts.toDF("Country","Salary").orderBy(asc("Country"))
    final_counts_dataframe.show(10)
    //final_counts_dataframe.repartition(1).write.format("csv").save("Gayathri_Ravichandran_task1.csv")
    //val b = final_counts_dataframe.select(col("Salary")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
    //print("total summmm")
    //print(b)
    val a = final_counts_dataframe.select(col("Salary")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
    val total_df = spark_context.parallelize(List( ("Total",a))).toDF("Country","Salary")
    //val df_res1 = total_df.join(final_counts_dataframe,"Country")
    total_df.show(10)
    val final_res_df = total_df.union(final_counts_dataframe)
    final_res_df.show(10)
    final_res_df.repartition(2).write.format("csv").save(output_file)



  }
}
