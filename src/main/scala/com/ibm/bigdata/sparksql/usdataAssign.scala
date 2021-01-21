package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object usdataAssign {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usdataAssign").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\us-500.csv"
    val usdf =spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    usdf.createOrReplaceTempView("usdata")

    val result = spark.sql("select (SUBSTRING_INDEX(SUBSTR(email, INSTR(email, '@') + 1),'.',1)) as domain,count(*) as count FROM usdata group by domain order by count desc")
    result.show()

    spark.stop()
  }
}