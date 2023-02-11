package com.strizhonovapps.prqtcomparator

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class ResultWriterFacade(hdfsResultWriter: HdfsResultWriter,
                         localResultWriter: LocalResultWriter) {
  private val dataComparisonResults = mutable.MutableList.empty[DataComparisonResult]

  def writeReport(): Unit = {
    try doWriteReport()
    finally flush()
  }

  def writeData(df: DataFrame, analyzedDirName: String): Unit = hdfsResultWriter.write(df, analyzedDirName)

  def addDataComparisonResult(dataComparisonResult: DataComparisonResult): Unit =
    dataComparisonResults += dataComparisonResult

  private def flush(): Unit = {
    localResultWriter.flush()
    this.dataComparisonResults.clear()
  }

  private def doWriteReport(): Unit = {
    localResultWriter.writeHeader()
    dataComparisonResults
      .sortWith(_.comparedDirName < _.comparedDirName)
      .foreach(write)
    val failedComparisonResults = dataComparisonResults
      .filter(_.isInstanceOf[FailedComparisonResult])
      .map(_.asInstanceOf[FailedComparisonResult])
      .toSet
    localResultWriter.writeFooter(failedComparisonResults)
  }

  private def write(dataComparisonResult: DataComparisonResult): Unit =
    dataComparisonResult match {
      case result: DefaultDataComparisonResult => localResultWriter.write(result)
      case result: DifferentSchemaResult => localResultWriter.write(result)
      case result: LeftEmptyDataFrameResult => localResultWriter.write(result)
      case result: RightEmptyDataFrameResult => localResultWriter.write(result)
      case _: FailedComparisonResult => //skip
      case _: BothEmptyDataFramesResult => throw new IllegalArgumentException("Unexpected data comparison result.")
    }
}
