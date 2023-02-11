package com.strizhonovapps.prqtcomparator

case class InputArgs(leftPath: String,
                     rightPath: String,
                     localResultPath: String,
                     hdfsResultPath: String,
                     primaryKeyCols: Seq[String],
                     excludedCols: Seq[String])

/////////////////////////////////////////////////////////////////////////////////////////

sealed trait DataComparisonResult {

  def isInvalid: Boolean = !isValid

  def isValid: Boolean

  def comparedDirName: String

}

case class DefaultDataComparisonResult(comparedDirName: String,
                                       leftSourceDfCount: Long,
                                       rightSourceDfCount: Long,
                                       differentRecordsCount: Long) extends DataComparisonResult {
  def isValid: Boolean = differentRecordsCount == 0
}

case class DifferentSchemaResult(comparedDirName: String,
                                 leftSourceDfCount: Long,
                                 rightSourceDfCount: Long,
                                 leftColNames: Array[String],
                                 rightColNames: Array[String],
                                 message: String) extends DataComparisonResult {
  override def isValid: Boolean = false
}

case class BothEmptyDataFramesResult(comparedDirName: String) extends DataComparisonResult {
  override def isValid: Boolean = true
}

case class LeftEmptyDataFrameResult(comparedDirName: String, rightSourceDfCount: Long) extends DataComparisonResult {
  override def isValid: Boolean = false
}

case class RightEmptyDataFrameResult(comparedDirName: String, leftSourceDfCount: Long) extends DataComparisonResult {
  override def isValid: Boolean = false
}

case class FailedComparisonResult(comparedDirName: String) extends DataComparisonResult {
  override def isValid: Boolean = false
}
