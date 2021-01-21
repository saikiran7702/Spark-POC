package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object import_all {

//   my sql db
  val murl ="jdbc:mysql://mysqldb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:3306/mysqldb"
  import java.util.Properties
  val mprop = new Properties()
  mprop.setProperty("user","myusername")
  mprop.setProperty("password","mypassword")
  mprop.setProperty("driver","com.mysql.jdbc.Driver")

//  oracle db
  val ourl ="jdbc:oracle:thin:@//oracledb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new Properties()
  oprop.setProperty("user","ousername")
  oprop.setProperty("password","opassword")
  oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")


// ms sql db
  val msurl ="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
  val msprop = new Properties()
  msprop.setProperty("user","msuername")
  msprop.setProperty("password","mspassword")
  msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")


}