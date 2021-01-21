package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oracleandmssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracle").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

//    ms sql db
    val msurl = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val mssqldf = spark.read.format("jdbc")
      .option("user","msuername")
      .option("password","mspassword")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable","dept")
      .option("url",msurl).load()

    mssqldf.show()


// oracle db
    val ourl = "jdbc:oracle:thin:@//oracledb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:1521/ORCL"
    import java.util.Properties
    val prop = new Properties()

    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
    val oracledf = spark.read.jdbc(ourl, "EMP",prop)
    oracledf.show()

//    join condition
    mssqldf.createOrReplaceTempView("dept_table")
    oracledf.createOrReplaceTempView("EMP_table")

    val join = spark.sql("select EMP_table.*,dept_table.loc,dept_table.dname from " +
      "dept_table join EMP_table on EMP_table.deptno=dept_table.deptno")
    join.show()

//    to write data in mysql db

    val murl ="jdbc:mysql://mysqldb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:3306/mysqldb"
    // import java.util.Properties
    val mprop = new Properties()
    mprop.setProperty("user","myusername")
    mprop.setProperty("password","mypassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
    join.write.mode(SaveMode.Overwrite).jdbc(murl,"EMP_DEPT_join",mprop)




    spark.stop()
  }
}