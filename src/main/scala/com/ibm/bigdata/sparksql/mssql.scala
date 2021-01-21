package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mysql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
// one way to read data from anu data base
    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val df = spark.read.format("jdbc")
      .option("user","msuername")
      .option("password","mspassword")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable","abhidf")
      .option("url",url).load()

//    second way -- java/scala
    import java.util.Properties
    val prop = new Properties()

    prop.setProperty("user","msuername")
    prop.setProperty("password","mspassword")
    prop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val msdf = spark.read.jdbc(url, "abhidf",prop)
    msdf.show()



    //df.show()



    spark.stop()
  }
}