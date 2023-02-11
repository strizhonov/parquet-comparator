package com.strizhonovapps.prqtcomparator

object ComparatorUtil {

  def matchesAtLeastOnePattern(test: String, patterns: Seq[String]): Boolean =
    patterns.map(test.matches).reduceOption(_ || _).getOrElse(false)

  def fixDirPath(dirPath: String): String = if (!dirPath.endsWith("/")) dirPath.concat("/") else dirPath

  def backticks(string: String, strings: String*): String =
    (string +: strings)
      .map(s => if (s.contains(".") && !s.startsWith("`") && !s.endsWith("`")) s"`$s`" else s)
      .mkString(".")
}
