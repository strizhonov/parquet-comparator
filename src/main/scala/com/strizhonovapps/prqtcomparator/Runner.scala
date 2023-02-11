package com.strizhonovapps.prqtcomparator

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object Runner {

  private val LeftPathParamName = "firstPath"
  private val RightPathParamName = "secondPath"
  private val LocalResultPathParamName = "localResultPath"
  private val HdfsResultPathParamName = "hdfsResultPath"
  private val PrimaryKeysParamName = "primaryKeys"
  private val ExcludedColsParamName = "excluded"

  def compare(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val start = LocalDateTime.now
    println(start)

    val args = getInputArgs(sc)
    val context = AppContext.init(sc, sqlContext, args)
    context.get(classOf[CoreComparatorService]).compare()

    println(s"Analysis duration, minutes: ${ChronoUnit.MINUTES.between(start, LocalDateTime.now)}")
  }

  private def getInputArgs(sc: SparkContext): InputArgs = {
    val args = sc.getConf.get("spark.driver.args").split("\\s+").toSeq
    val leftPath = getFromArgs(args, LeftPathParamName).get
    val rightPath = getFromArgs(args, RightPathParamName).get
    val localResultPath = getFromArgs(args, LocalResultPathParamName).get
    val hdfsResultPath = getFromArgs(args, HdfsResultPathParamName).get
    val primaryKeys = getFromArgs(args, PrimaryKeysParamName).map(_.split(",")).getOrElse(Array.empty)
    val excludedCols = getFromArgs(args, ExcludedColsParamName).map(_.split(",")).getOrElse(Array.empty)
    InputArgs(
      ComparatorUtil.fixDirPath(leftPath),
      ComparatorUtil.fixDirPath(rightPath),
      ComparatorUtil.fixDirPath(localResultPath),
      ComparatorUtil.fixDirPath(hdfsResultPath),
      primaryKeys,
      excludedCols
    )
  }

  private def getFromArgs(args: Seq[String], param: String): Option[String] =
    args.filter(_.startsWith(s"--$param=")).reduceOption((first, _) => first).map(_.replace(s"--$param=", ""))
}
