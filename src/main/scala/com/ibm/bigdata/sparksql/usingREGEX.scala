package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object usingREGEX {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usingREGEX").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\bigdata\\datasets\\10000Records.csv"
    val regex = "[^a-zA-z]"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    //df.show(5)
    // to clean header with multiple spaces and other expressions
    val cleancolumn = df.columns.map(x=>x.replaceAll(regex,""))
    // As the base data frame is not clean we assigned the columns to a new val called cleancolumn
    // now we have cleaned columns and data seprately we need to append the columns names to the  data
    val newdataDF = df.toDF(cleancolumn:_*)
    newdataDF.show(5)


    spark.stop()
  }
}