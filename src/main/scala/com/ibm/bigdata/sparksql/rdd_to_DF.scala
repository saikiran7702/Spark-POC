package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object rdd_to_DF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rdd_to_DF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\bank-full.csv"


    val df = spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", ";")
            .option("inferSchema","true")
            .load(data)
    //df.show(5)
    df.createOrReplaceTempView("sai")
    val result = spark.sql("select * from sai limit 10")
    result.show()
    result.printSchema()

    spark.stop()
  }
}