package com.strizhonovapps.prqtcomparator

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkSession {
  
  val sparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("AppName")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    SparkSession.builder().config(conf).getOrCreate()
  }
  
}