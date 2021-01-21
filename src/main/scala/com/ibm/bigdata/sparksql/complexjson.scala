package com.ibm.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.ibm.bigdata.sparksql.import_all._

object complexjson {



  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexjson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\bigdata\\datasets\\world_bank.json"

    val df = spark.read.format("json").load(data)

    // struct data handling, we use this when we have parent_column.child_column
    // explode is used to convert array data type to struct or access elements inside array data type



    val newdf = df.withColumn("theme1name",$"theme1.Name")
      //.withColumn("theme1Percent",$"theme1.Percent")
      //.withColumn("theme_namecode",explode($"theme_namecode"))
      .withColumn("majorsector_percent",explode($"majorsector_percent"))
      .withColumn("mjsector_namecode",explode($"mjsector_namecode"))
      .withColumn("mjtheme_namecode",explode($"mjtheme_namecode"))
      .withColumn("projectdocs",explode($"projectdocs"))
      .withColumn("sector",explode($"sector"))
      .withColumn("sector_namecode",explode($"sector_namecode"))
      .withColumn("theme_namecode",explode($"theme_namecode"))
      .withColumn("idoid",$"_id.$$oid")



    //newdf.select("theme1","theme1name","theme1Percent","theme_namecode","theme_namecode.code").show(10,false)


    // to convert struct to regular colunms
   val res1 = newdf.select($"*",$"projectdocs.*",
     $"majorsector_percent.Name".alias("majorsector_percent_Name"),
     $"majorsector_percent.Percent".alias("majorsector_percent_Name"),
     $"mjsector_namecode.code".alias("mjsector_namecode_code"),
     $"mjsector_namecode.name".alias("mjsector_namecode_name"),
     $"mjtheme_namecode.code".alias("mjtheme_namecode_code"),
     $"mjtheme_namecode.name".alias("mjtheme_namecode_name"),
     $"project_abstract.cdata".alias("project_abstract_cdata"),
     $"sector.name".alias("sector_name"),
     $"sector1.name".alias("sector1_name"),
     $"sector1.Percent".alias("Percent"),
     $"sector2.name".alias("sector2_name"),
     $"sector2.percent".alias("sector2.percent"),
     $"sector3.name".alias("sector3_name"),
     $"sector3.Percent".alias("sector3_percent"),
     $"sector4.name".alias("sector4_name"),
     $"sector4.Percent".alias("sector4_percent"),
     $"sector_namecode.code".alias("sector_namecode_code"),
     $"sector_namecode.name".alias("sector_namecode_name"),
     $"theme_namecode.code".alias("theme_namecode_code"),
     $"theme_namecode.name".alias("theme_namecode.name"),
     explode($"mjtheme").alias("mjthemedata"))

      // to remove all the struct type parent column after extracting the child column
     .drop("mjtheme","_id","projectdocs","theme_namecode",
       "project_abstract","mjsector_namecode","mjtheme_namecode",
       "sector_namecode","sector4","sector3","sector2","sector1","sector",
       "majorsector_percent")



    res1.show(10,false)
    res1.printSchema()


    // to store this data in any data base as i aleady have import all object we can use it

//    res1.write.jdbc(msurl,"jsondata",msprop)
//    println("mssql completed")
//    res1.write.jdbc(ourl,"jsonora",oprop)


    spark.stop()
  }
}