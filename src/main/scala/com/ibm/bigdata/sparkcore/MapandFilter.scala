package com.ibm.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MapandFilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("MapandFilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
//val num = Array(1,2,3,4,5,6,7,8)
//    val nrdd = sc.parallelize(num)
//    val result = nrdd.map(x=> x*x)
//    result.collect().foreach(println)

    val data ="C:\\bigdata\\datasets\\asl.csv"
    val aslrdd = sc.textFile(data)
    //select * from tab where city=...
    //select city, count(*) from tab group by city
    //select join two tables
    // val res = aslrdd.filter(x=>x.contains("blr"))
    val skip = aslrdd.first()

    val res = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3))).filter(x=>x._3=="blr" && x._2<30)
    //array(jyo,12,blr,fri)
    //array(koti,29,blr,saturday)
    res.collect.foreach(println)


    spark.stop()

  }
}