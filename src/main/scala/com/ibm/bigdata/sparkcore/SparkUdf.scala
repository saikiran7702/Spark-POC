package com.ibm.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkUdf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkUdf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data ="C:\\bigdata\\datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",";").load(data)

    val res = df.where($"balance">=60000 and $"marital"==="married")// domain spefic launguage

    //concat : combines strings directly concat_ws combines strings with a common seperator
    //in python: as we need to import "import org.apache.spark.sql.functions._" as F instead of $--> f.col("job") nd
    // use lit for seperator

//    val res = df.withColumn("name",concat($"job",$"education",$"contact"))
//      .withColumn("name_2", concat_ws("_",$"job",$"education",$"contact"))
//      res.show(truncate = false)
//    val res = df.withColumn("job",regexp_replace($"job","-",""))

//    regexp_replace: can be used for minimal replacement
//    when we need to replace many items in a data set use "when"
//    val res = df.withColumn("marital",when($"marital"==="married","together")
//            .when($"marital"==="single","bachelor")
//            .when($"marital"==="divorced","seperated")
//            .otherwise($"marital"))






    res.show(truncate = false)



    spark.stop()
  }
}