package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkHive {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkHive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
//    when ever we need to use .enableHiveSupport()in the spark session
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = args(0)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("sai")

    val result = spark.sql("select * from sai where state ='LA'")

    df.show(5)

    val table = args(1)
    val msurl="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"

    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    //result.write.jdbc(msurl,"s3tomssql",msprop)
    //    if we do not want to hardcode the table name follow below code
    result.write.mode(SaveMode.Overwrite).jdbc(msurl,table,msprop)
    result.write.mode(SaveMode.Overwrite).saveAsTable(table)

    spark.stop()
  }
}