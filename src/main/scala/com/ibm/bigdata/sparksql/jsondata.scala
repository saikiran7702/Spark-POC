package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object jsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\bigdata\\datasets\\zips.json"
     val df = spark.read.format("json").load(data)
    //df.show()


//    dsl approach
    //val res = df.select($"id".alias("id"),$"city")
    // with column creates a new colounm if there no column does not exist; if there is a it updates the column with the value
    val result = df.withColumn("age",lit("18"))
      .withColumn("city",when($"city"==="BLANDFORD","BLAN").otherwise($"city"))
      //.withColumn("state",regexp_replace("$state","MA","MAAA"))

    spark.stop()
  }
}