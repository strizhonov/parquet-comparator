package com.strizhonovapps.prqtcomparator

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import scala.collection.mutable

object AppContext {

  private val RecordsLimit = 100
  private val LeftAlias = "left"
  private val RightAlias = "right"
  private val DiffTypeColName = "diff_type"
  private val ChangedColumnsColName = "changed_columns"
  private val InsertTypeLiteral = "I"
  private val DeleteTypeLiteral = "D"
  private val ChangeTypeLiteral = "C"
  private val ResultFileName = "comparison_result"

  def init(sc: SparkContext, sqlContext: SQLContext, args: InputArgs): AppContext = {
    val printWriter = createPrintWriter(args.localResultPath)
    val localResultWriter = createLocalResultWriter(printWriter, args)
    val hdfsResultWriter = createHdfsResultWriter(sc, args)
    val resultWriterFacade = createResultWriterFacade(localResultWriter, hdfsResultWriter)
    val dataFrameComparator = createDataFrameComparator()
    val dataComparisonCreator = createDataComparisonCreator(dataFrameComparator, args)
    val parquetComparator = createParquetComparator(sc, sqlContext, args, dataComparisonCreator, resultWriterFacade)
    new AppContext()
      .put(printWriter)
      .put(localResultWriter)
      .put(hdfsResultWriter)
      .put(resultWriterFacade)
      .put(dataFrameComparator)
      .put(dataComparisonCreator)
      .put(parquetComparator)
  }

  private def createHdfsResultWriter(sc: SparkContext, args: InputArgs): HdfsResultWriter =
    new HdfsResultWriter(
      sc,
      args.hdfsResultPath,
      RecordsLimit
    )

  private def createParquetComparator(sc: SparkContext,
                                      sqlContext: SQLContext,
                                      args: InputArgs,
                                      dataComparisonCreator: DataComparisonCreator,
                                      resultWriter: ResultWriterFacade): CoreComparatorService = {
    new CoreComparatorService(
      sqlContext = sqlContext,
      hdfs = FileSystem.get(sc.hadoopConfiguration),
      resultWriter = resultWriter,
      dataComparisonCreator = dataComparisonCreator,
      leftParquetDir = args.leftPath,
      rightParquetDir = args.rightPath
    )
  }

  private def createDataComparisonCreator(dataFrameComparator: DataFrameComparator, args: InputArgs): DataComparisonCreator = {
    val primaryKeys = args.primaryKeyCols
    val excludedColNamePatterns = args.excludedCols
    new DataComparisonCreator(dataFrameComparator, primaryKeys, excludedColNamePatterns)
  }

  private def createResultWriterFacade(localResultWriter: LocalResultWriter, hdfsResultWriter: HdfsResultWriter): ResultWriterFacade =
    new ResultWriterFacade(hdfsResultWriter, localResultWriter)

  private def createDataFrameComparator(): DataFrameComparator =
    new DataFrameComparator(
      DiffTypeColName,
      ChangedColumnsColName,
      LeftAlias,
      RightAlias,
      InsertTypeLiteral,
      DeleteTypeLiteral,
      ChangeTypeLiteral
    )

  private def createLocalResultWriter(printWriter: PrintWriter, args: InputArgs) =
    new LocalResultWriter(printWriter, args.hdfsResultPath)

  private def createPrintWriter(localResultPath: String): PrintWriter = {
    val outputStream = new FileOutputStream(localResultPath + s"$ResultFileName.txt")
    val writer = new OutputStreamWriter(outputStream, "UTF-8")
    new PrintWriter(writer)
  }
}

class AppContext(map: mutable.Map[Class[_], Any]) {
  def this() = this(mutable.Map.empty)

  def get[T](clazz: Class[T]): T = map(clazz).asInstanceOf[T]

  def put(obj: Any): AppContext = {
    map += (obj.getClass -> obj)
    this
  }
}