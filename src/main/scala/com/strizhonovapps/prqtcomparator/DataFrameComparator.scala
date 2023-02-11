package com.strizhonovapps.prqtcomparator

import com.strizhonovapps.prqtcomparator.ComparatorUtil.backticks
import org.apache.spark.sql.functions.{coalesce, concat_ws, lit, when}
import org.apache.spark.sql.{Column, DataFrame}

class DataFrameComparator(diffTypeColName: String,
                          changedColumnsColName: String,
                          leftAlias: String,
                          rightAlias: String,
                          insertTypeLiteral: String,
                          deleteTypeLiteral: String,
                          changeTypeLiteral: String) {

  def safeCompare(leftDf: DataFrame,
                  rightDf: DataFrame,
                  pkColNames: Seq[String] = Seq.empty,
                  excludedColNames: Seq[String] = Seq.empty): DataFrame = {
    val clearedLeftDfColNames = leftDf.columns.filter(col => !excludedColNames.contains(col))
    val clearedRightDfColNames = rightDf.columns.filter(col => !excludedColNames.contains(col))
    val validationResult = getErrorMessageIfSchemasInvalid(clearedLeftDfColNames, clearedRightDfColNames, pkColNames)
    if (validationResult.isDefined) throw new IllegalArgumentException(validationResult.get)
    this.compare(leftDf, rightDf, clearedLeftDfColNames, pkColNames)
  }

  def compare(leftDf: DataFrame,
              rightDf: DataFrame,
              nonIgnorableColNames: Seq[String],
              pkColNames: Seq[String] = Seq.empty): DataFrame = {
    val validatedPkColNames = if (pkColNames.isEmpty) nonIgnorableColNames.toList else pkColNames
    val valueColNames = nonIgnorableColNames.filter(col => !validatedPkColNames.contains(col))
    val (clearedLeftDf, clearedRightDf) = clearDataframes(leftDf, rightDf, validatedPkColNames ++ valueColNames)
    val joinByPrimaryKeys = validatedPkColNames.map(c => clearedLeftDf(backticks(c)) <=> clearedRightDf(backticks(c))).reduce(_ && _)
    val diffTypeCol =
      when(allNull(clearedLeftDf, validatedPkColNames ++ valueColNames), lit(insertTypeLiteral))
        .when(allNull(clearedRightDf, validatedPkColNames ++ valueColNames), lit(deleteTypeLiteral))
        .otherwise(lit(changeTypeLiteral))
        .as(diffTypeColName)
    val changedColNamesCol = getChangedColNamesColumn(validatedPkColNames, valueColNames, clearedLeftDf, clearedRightDf)
    val pkAndValueCols = mapNamesToCols(validatedPkColNames, valueColNames, clearedLeftDf, clearedRightDf)
    clearedLeftDf.join(clearedRightDf, joinByPrimaryKeys, "fullouter")
      .select(diffTypeCol +: (changedColNamesCol +: pkAndValueCols): _*)
  }

  def getErrorMessageIfSchemasInvalid(leftDfColNames: Seq[String],
                                      rightDfColNames: Seq[String],
                                      pkColNames: Seq[String]): Option[String] = {
    if (leftDfColNames.length != leftDfColNames.toSet.size || rightDfColNames.length != rightDfColNames.toSet.size) {
      return Some(
        "The datasets have duplicate columns.\n" +
          s"Left column names: ${leftDfColNames.mkString("::")}\n" +
          s"Right column names: ${rightDfColNames.mkString("::")}"
      )
    }
    if (leftDfColNames.length != rightDfColNames.length) {
      return Some(
        "The number of columns doesn't match.\n" +
          s"Left column names (${leftDfColNames.length}): ${leftDfColNames.mkString("::")}\n" +
          s"Right column names (${rightDfColNames.length}): ${rightDfColNames.mkString("::")}"
      )
    }
    if (leftDfColNames.isEmpty) {
      return Some("The schema must not be empty")
    }
    if (leftDfColNames != rightDfColNames) {
      return Some(
        "The datasets do not have the same schema.\n" +
          s"Left columns: ${leftDfColNames.mkString("::")}\n" +
          s"Right columns: ${rightDfColNames.mkString("::")}"
      )
    }
    val colNames = leftDfColNames
    val validatedPkColNames = if (pkColNames.isEmpty) colNames.toList else pkColNames
    val missingPkColNames = validatedPkColNames.diff(colNames)
    if (missingPkColNames.nonEmpty) {
      return Some(
        s"Some pk columns do not exist: ${missingPkColNames.mkString(", ")} missing among ${colNames.mkString("::")}"
      )
    }
    if (validatedPkColNames.contains(diffTypeColName)) {
      return Some(
        s"The pk columns must not contain the diffType column name '$diffTypeColName': " + validatedPkColNames.mkString("::")
      )
    }
    if (changedColumnsColName.exists(validatedPkColNames.contains)) {
      return Some(s"The pk columns must not contain the changed columns column name '$changedColumnsColName': ${validatedPkColNames.mkString("::")}")
    }
    None
  }

  private def clearDataframes(leftDf: DataFrame, rightDf: DataFrame, neededColNames: Seq[String]): (DataFrame, DataFrame) = {
    val neededCols = neededColNames.map(new Column(_))
    val withNeededColsLeftDf = leftDf.select(neededCols: _*)
    val withNeededColsRightDf = rightDf.select(neededCols: _*)
    val sameValueRows = withNeededColsLeftDf.intersect(withNeededColsRightDf)
    val clearedLeftDf = withNeededColsLeftDf.except(sameValueRows)
    val clearedRightDf = withNeededColsRightDf.except(sameValueRows)
    (clearedLeftDf, clearedRightDf)
  }

  private def allNull(left: DataFrame, allColNames: Seq[String]): Column =
    allColNames
      .map(colName => left(colName).isNull)
      .reduce(_ && _)

  private def getChangedColNamesColumn(pkColNames: Seq[String], valueColNames: Seq[String],
                                       leftDf: DataFrame, rightDf: DataFrame): Column = {
    val otherwise = Some(valueColNames)
      .filter(_.nonEmpty)
      .map(columns => concat_ws("::", getColNameIfUnequal(columns, leftDf, rightDf): _*))
      .getOrElse(lit())
    when(allNull(leftDf, pkColNames ++ valueColNames) || allNull(rightDf, pkColNames ++ valueColNames), lit(""))
      .otherwise(otherwise)
      .as(changedColumnsColName)
  }

  private def getColNameIfUnequal(colNames: Seq[String], leftDf: DataFrame, rightDf: DataFrame): Seq[Column] =
    colNames.map(c => when(leftDf(backticks(c)) <=> rightDf(backticks(c)), lit("")).otherwise(lit(c)))

  private def mapNamesToCols(pkColNames: Seq[String], valueColNames: Seq[String],
                             leftDf: DataFrame, rightDf: DataFrame): Seq[Column] = {
    val pkCols = pkColNames.map(c => coalesce(leftDf(backticks(c)), rightDf(backticks(c))).as(c))
    val leftValues = valueColNames.map(c => (c, leftDf(backticks(c)))).toMap
    val rightValues = valueColNames.map(c => (c, rightDf(backticks(c)))).toMap
    val valueCols = valueColNames.flatMap(c => Seq(
      leftValues(c).as(s"${leftAlias}_$c"),
      rightValues(c).as(s"${rightAlias}_$c")
    ))
    pkCols ++ valueCols
  }
}