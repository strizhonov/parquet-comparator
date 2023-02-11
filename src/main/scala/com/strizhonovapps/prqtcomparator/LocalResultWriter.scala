package com.strizhonovapps.prqtcomparator

import java.io.PrintWriter

class LocalResultWriter(printWriter: PrintWriter,
                        hdfsResultPath: String) {
  def write(result: DefaultDataComparisonResult): Unit = {
    printWriter.write(
      s"[${result.comparedDirName}] discrepancies:\n" +
        s"\t[INFO] #1 dataset count: ${result.leftSourceDfCount}\n" +
        s"\t[INFO] #2 dataset count: ${result.rightSourceDfCount}\n" +
        s"\t[WARN] ${result.differentRecordsCount} records have data discrepancies.\n" +
        s"\t\tDiscrepancies' HDFS path: $hdfsResultPath${result.comparedDirName}\n" +
        "\n" +
        "\n"
    )
  }

  def write(result: DifferentSchemaResult): Unit = {
    printWriter.write(
      s"[${result.comparedDirName}] discrepancies:\n" +
        s"\t[INFO] #1 dataset count: ${result.leftSourceDfCount}\n" +
        s"\t[INFO] #2 dataset count: ${result.rightSourceDfCount}\n" +
        s"\t[WARN] #1 dataset schema differs from #2 dataset schema.\n" +
        s"\t\t #1 schema: ${result.leftColNames.mkString("::")}\n" +
        s"\t\t #2 schema: ${result.rightColNames.mkString("::")}\n" +
        "\n" +
        "\n"
    )
  }

  def write(result: LeftEmptyDataFrameResult): Unit = {
    printWriter.write(s"[${result.comparedDirName}] discrepancies:\n" +
      s"\t[INFO] #2 dataset count: ${result.rightSourceDfCount}\n" +
      s"\t[WARN] #1 dataset is empty.\n" +
      "\n" +
      "\n"
    )
  }

  def write(result: RightEmptyDataFrameResult): Unit = {
    printWriter.write(
      s"[${result.comparedDirName}] discrepancies:\n" +
        s"\t[INFO] #1 dataset count: ${result.leftSourceDfCount}\n" +
        s"\t[WARN] #2 dataset is empty.\n" +
        "\n" +
        "\n"
    )
  }

  def writeHeader(): Unit = {
    printWriter.write(
      "[INFO]: Valid data information is omitted.\n" +
        "**********************************************************************************\n" +
        "\n"
    )
  }

  def writeFooter(failedTypes: Set[FailedComparisonResult] = Set.empty): Unit = {
    printWriter.write(
      "**********************************************************************************\n" +
        "[INFO]: Parquet comparison finished.\n")
    failedTypes.foreach(failedType => printWriter.write(s"[ERROR] Directory [${failedType.comparedDirName}] comparison failed.\n"))
  }

  def flush(): Unit = printWriter.flush()
}
