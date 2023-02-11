package com.strizhonovapps.prqtcomparator

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.util.concurrent.atomic.AtomicInteger

class CoreComparatorService(sqlContext: SQLContext,
                            hdfs: FileSystem,
                            resultWriter: ResultWriterFacade,
                            dataComparisonCreator: DataComparisonCreator,
                            leftParquetDir: String,
                            rightParquetDir: String) {
  private val comparedDirCounter = new AtomicInteger
  private val dirsToCompareCount = new AtomicInteger

  def compare(): Unit = {
    val dirsToCompare = getDirsToCompare
    dirsToCompareCount.set(dirsToCompare.size)
    dirsToCompare
      .map(getComparisonResultWithDfSaving)
      .filter(_.isInvalid)
      .foreach(resultWriter.addDataComparisonResult)
    resultWriter.writeReport()

    comparedDirCounter.set(0)
    dirsToCompareCount.set(0)

    println("Parquet comparison finished successfully.")
  }

  private def getComparisonResultWithDfSaving(dirNameToCompare: String): DataComparisonResult = {
    val (leftDf, rightDf) = getDataFrames(dirNameToCompare)
    val resultTuple = dataComparisonCreator.getDataComparisonResult(leftDf, rightDf, dirNameToCompare)
    if (resultTuple._2.isDefined) {
      resultWriter.writeData(
        resultTuple._2.get,
        dirNameToCompare
      )
      sqlContext.clearCache()
    }
    logProgress()
    resultTuple._1
  }

  private def logProgress(): Unit = {
    val progressValue = comparedDirCounter.incrementAndGet().toDouble / dirsToCompareCount.get() * 100
    println(s"--------------> Current progress: ${progressValue.toInt}%")
  }

  private def getDataFrames(dirNameToCompare: String): (Option[DataFrame], Option[DataFrame]) = {
    val fixedDirName =
      if (dirNameToCompare.startsWith("/"))
        dirNameToCompare.replaceFirst("/", "")
      else
        dirNameToCompare
    val leftPath = leftParquetDir.concat(fixedDirName)
    val leftDf = getDataFrameFromPath(leftPath)
    val rightPath = rightParquetDir.concat(fixedDirName)
    val rightDf = getDataFrameFromPath(rightPath)
    (leftDf, rightDf)
  }

  private def getDirsToCompare: Set[String] = {
    val leftSubTypes = getDirs(leftParquetDir)
    val rightSubTypes = getDirs(rightParquetDir)
    leftSubTypes.union(rightSubTypes)
  }

  private def getDirs(path: String): Set[String] = {
    hdfs.listStatus(new Path(path)).filter(_.isDirectory)
      .map(_.getPath.toString)
      .toSet
  }

  private def getDataFrameFromPath(path: String): Option[DataFrame] =
    try {
      if (hdfs.exists(new Path(path))) Some(sqlContext.read.parquet(path).cache()) else Option.empty
    } catch {
      case _: AssertionError =>
        println(s"Unable to load data from $path. Probably the folder is empty.")
        Option.empty
    }
}