package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.ibm.bigdata.sparksql.import_all._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object oracleandmssql_join {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracleandmssql_join").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

//  oracle db EMP table
    val oracle_df = spark.read.jdbc(ourl,"EMP",oprop)
    oracle_df.show()

//



    spark.stop()
  }
}