package com.strizhonovapps.prqtcomparator

import org.apache.spark.sql.DataFrame

class DataComparisonCreator(dataFrameComparator: DataFrameComparator,
                            primaryKeyCols: Seq[String],
                            excludedColNamePatterns: Seq[String]) {

  def getDataComparisonResult(leftDf: Option[DataFrame],
                              rightDf: Option[DataFrame],
                              dirName: String): (DataComparisonResult, Option[DataFrame]) =
    try {
      if (leftDf.isEmpty && rightDf.isEmpty) return (BothEmptyDataFramesResult(dirName), None)
      if (leftDf.isEmpty) return (LeftEmptyDataFrameResult(dirName, rightDf.get.count), None)
      if (rightDf.isEmpty) return (RightEmptyDataFrameResult(dirName, leftDf.get.count), None)
      val excludedColNames = (leftDf.get.columns ++ rightDf.get.columns)
        .filter(col => ComparatorUtil.matchesAtLeastOnePattern(col, excludedColNamePatterns))
      val clearedLeftDfColNames = leftDf.get.columns.filter(col => !excludedColNames.contains(col))
      val clearedRightDfColNames = rightDf.get.columns.filter(col => !excludedColNames.contains(col))
      val schemaErrorMessage = dataFrameComparator.getErrorMessageIfSchemasInvalid(clearedLeftDfColNames, clearedRightDfColNames, primaryKeyCols)
      if (schemaErrorMessage.isDefined) {
        println(s"Wrong schema for directory: $dirName. ${schemaErrorMessage.get}")
        return (
          DifferentSchemaResult(dirName, leftDf.get.count(), rightDf.get.count(), leftDf.get.columns, rightDf.get.columns, schemaErrorMessage.get),
          None
        )
      }
      val comparedDf = dataFrameComparator.compare(leftDf.get, rightDf.get, clearedLeftDfColNames, primaryKeyCols).cache()
      val resultDfCount = comparedDf.count
      (
        DefaultDataComparisonResult(dirName, leftDf.get.count, rightDf.get.count, resultDfCount),
        if (resultDfCount == 0) None else Some(comparedDf)
      )
    } catch {
      case e: Throwable =>
        println(s"Unable to write $dirName}")
        e.printStackTrace()
        (FailedComparisonResult(dirName), None)
    }
}
