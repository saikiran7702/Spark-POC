package com.ibm.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object flatmap {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("flatmap").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\bigdata\\datasets\\wcdata.txt"
    val flatmaprdd = sc.textFile(data)
    val rdd1 = flatmaprdd.flatMap(x=>x.split(" "))
      rdd1.take(15).foreach(println)


    spark.stop()
  }
}