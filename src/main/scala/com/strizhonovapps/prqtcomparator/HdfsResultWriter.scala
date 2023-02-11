package com.strizhonovapps.prqtcomparator

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

class HdfsResultWriter(sc: SparkContext,
                       hdfsResultPath: String,
                       recordsLimit: Int) {
  def write(df: DataFrame, analyzedDirName: String): Unit = {
    println(s"Writing json files for dir [$analyzedDirName]")
    prepareForSave(df).write.json(hdfsResultPath + analyzedDirName)
  }

  private def prepareForSave(df: DataFrame): DataFrame = {
    val partitionAccumulator = sc.accumulator(0)
    df.foreachPartition(partition => if (partition.nonEmpty) partitionAccumulator.add(1))
    val nonEmptyPartitions = if (partitionAccumulator.localValue == 0) 1 else partitionAccumulator.localValue
    df.limit(recordsLimit).coalesce(nonEmptyPartitions)
  }
}
