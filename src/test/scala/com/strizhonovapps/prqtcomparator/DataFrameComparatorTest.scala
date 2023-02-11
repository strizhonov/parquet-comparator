package com.strizhonovapps.prqtcomparator

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, functions}
import org.junit.Assert.assertEquals
import org.junit.Test
import TestSparkSession.sparkSession

class DataFrameComparatorTest extends Serializable {

  @Test
  def `compare dataframes, another case`(): Unit = {
    val spark = SQLContext.getOrCreate(sparkSession.sparkContext)
    val leftList = List(
      Tuple2("1", "1"),
      Tuple2("2", "2"),
      Tuple2("3", "3"),
      Tuple2("4", "4"),
      Tuple2("5", "5"),
      Tuple2("6", "6")
    )
    val rightList = List(
      Tuple2("2", "3"),
      Tuple2("2", "4"),
      Tuple2("2", "2"),
      Tuple2("1", "3"),
      Tuple2("1", "6"),
      Tuple2("6", "6")
    )
    val leftDf = spark.createDataFrame(sparkSession.sparkContext.parallelize(leftList))
    val rightDf = spark.createDataFrame(sparkSession.sparkContext.parallelize(rightList))
    val counts = getResult(leftDf, rightDf)
    assertEquals(2, counts.filter(row => row.getAs[String](0) == "I")(0).getAs[Long](1))
    assertEquals(3, counts.filter(row => row.getAs[String](0) == "D")(0).getAs[Long](1))
    assertEquals(2, counts.filter(row => row.getAs[String](0) == "C")(0).getAs[Long](1))
  }

  @Test
  def `comparing dataframes with complex primary key`(): Unit = {
    val spark = SQLContext.getOrCreate(sparkSession.sparkContext)
    val leftList = List(
      Tuple4("key1", "key2", "value1", "value2"),
      Tuple4("key11", "key22", "value11", "value22"),
      Tuple4("key111", "key222", "value111", "value222"),
      Tuple4("key1111", "key2222", "value1111", "value2222"),
      Tuple4("key11111", "key22222", "vale11111", "value22222")
    )
    val rightList = List(
      Tuple4("key1", "key2", "value1", "value2"),
      Tuple4("key1", "key2", "value11", "value22"),
      Tuple4("key1", "key3", "value1", "value2"),
      Tuple4("key2", "key1", "value1", "value2"),
      Tuple4("key11111", "key22222", "DIFF", "value22222")
    )
    val leftDf = spark.createDataFrame(sparkSession.sparkContext.parallelize(leftList))
    val rightDf = spark.createDataFrame(sparkSession.sparkContext.parallelize(rightList))
    val counts = getResult(leftDf, rightDf)
    assertEquals(3, counts.filter(row => row.getAs[String](0) == "I")(0).getAs[Long](1))
    assertEquals(3, counts.filter(row => row.getAs[String](0) == "D")(0).getAs[Long](1))
    assertEquals(1, counts.filter(row => row.getAs[String](0) == "C")(0).getAs[Long](1))
  }

  @Test
  def `comparing dataframes with different but ignorable columns`(): Unit = {
    val spark = SQLContext.getOrCreate(sparkSession.sparkContext)
    val leftList = List(
      Tuple5("test1", "any_111", "any_222", "any_333", "any_A"),
      Tuple5("test2", "any_444", "any_555", "any_666", "any_B"),
      Tuple5("test3", "any_777", "any_888", "any_999", "any_C"),
      Tuple5("test6", "SAME_1", "SAME_2", "any_1111", "any_D")
    )
    val rightList = List(
      Tuple3("test5", "any_2222", "any_3333"),
      Tuple3("test4", "any_4444", "any_5555"),
      Tuple3("test3", "any_6666", "any_7777"),
      Tuple3("test3", "any_8888", "any_9999"),
      Tuple3("test6", "SAME_1", "SAME_2")
    )
    val leftDf = spark.createDataFrame(sparkSession.sparkContext.parallelize(leftList))
    val rightDf = spark.createDataFrame(sparkSession.sparkContext.parallelize(rightList))
    val counts = getResult(leftDf, rightDf, Seq("ignorable", "ignorableColumn"))
    assertEquals(2, counts.filter(row => row.getAs[String](0) == "I")(0).getAs[Long](1))
    assertEquals(2, counts.filter(row => row.getAs[String](0) == "D")(0).getAs[Long](1))
    assertEquals(2, counts.filter(row => row.getAs[String](0) == "C")(0).getAs[Long](1))
  }

  private def getResult(leftDf: DataFrame, rightDf: DataFrame, ignorable: Seq[String] = Seq.empty) = {
    val comparator = new DataFrameComparator("diff", "changed", "left", "right", "I", "D", "C")
    val result = comparator.safeCompare(leftDf, rightDf, Seq("key"), ignorable)
    val counts = result
      .groupBy(col("diff"))
      .agg(functions.count("*"))
      .as("counts")
      .collect()
    counts
  }

  case class Tuple2(key: String, anyValue: String)

  case class Tuple3(key: String, anyValue: String, anotherValue: String)

  case class Tuple4(key: String, anotherKey: String, anyValue: String, anotherValue: String)

  case class Tuple5(key: String, anyValue: String, anotherValue: String, ignorableColumn: String, ignorable: String)


}
