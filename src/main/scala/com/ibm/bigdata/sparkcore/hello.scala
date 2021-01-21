package com.ibm.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object hello {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("hello").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}

/* there are 3 environments available
1. spark shell -- testing and learning
2. intellij -- development
3. .jar -- production envinronment
 */

