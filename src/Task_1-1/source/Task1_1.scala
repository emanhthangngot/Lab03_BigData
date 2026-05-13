import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * Task 1-1: Sliding Window - Most Bought Size per State
 *
 * This solution follows the PDF's "Advanced MapReduce" requirement by using
 * Spark RDD transformations: mapPartitions, flatMap, reduceByKey, sortBy, and
 * saveAsTextFile. No DataFrame/Dataset query logic or spark.sql(...) is used.
 *
 * Query framing:
 * For every state and every sliding day d, look back at orders in [d-7, d-1].
 * A row contributes only when Status contains "shipped" case-insensitively and
 * Qty is positive. We sum Qty by Size and emit the Size with the largest total.
 */
case class RequiredColumns(date: Int, status: Int, qty: Int, size: Int, state: Int)
case class Purchase(orderEpochDay: Long, state: String, size: String, qty: Int)

object Task1_1 extends Serializable {

  private val DateColumn = "Date"
  private val StatusColumn = "Status"
  private val QtyColumn = "Qty"
  private val SizeColumn = "Size"
  private val StateColumn = "ship-state"

  private val OutputHeader = "ship-state,date,most_bought_size,max_quantity"

  /** 
   * Custom CSV Line Parser to avoid external libraries like univocity.
   * Handles quoted fields and commas inside quotes.
   */
  private def parseCsvLine(line: String): Array[String] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[String]
    var inQuotes = false
    var currentField = new java.lang.StringBuilder()
    var i = 0
    
    while (i < line.length) {
      val c = line.charAt(i)
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length && line.charAt(i + 1) == '"') {
             // Escaped quote: ""
             currentField.append('"')
             i += 1
          } else {
             inQuotes = false
          }
        } else {
          currentField.append(c)
        }
      } else {
        if (c == '"') {
          inQuotes = true
        } else if (c == ',') {
          buf += currentField.toString
          currentField.setLength(0)
        } else {
          currentField.append(c)
        }
      }
      i += 1
    }
    buf += currentField.toString
    buf.toArray
  }

  private def parseDate(value: String): Option[LocalDate] = {
    val DateFormatters = Seq(
      DateTimeFormatter.ofPattern("MM-dd-yy"),
      DateTimeFormatter.ofPattern("M-d-yy"),
      DateTimeFormatter.ofPattern("MM/dd/yy"),
      DateTimeFormatter.ofPattern("M/d/yy")
    )
    val trimmed = Option(value).getOrElse("").trim
    DateFormatters.iterator
      .map(formatter => scala.util.Try(LocalDate.parse(trimmed, formatter)).toOption)
      .collectFirst { case Some(date) => date }
  }

  private def parsePositiveInt(value: String): Option[Int] = {
    scala.util.Try(Option(value).getOrElse("").trim.toInt).toOption.filter(_ > 0)
  }

  private def requireTaskColumns(header: Array[String]): RequiredColumns = {
    val indexByColumn = header.zipWithIndex.map { case (name, index) => name.trim -> index }.toMap
    val required = Seq(DateColumn, StatusColumn, QtyColumn, SizeColumn, StateColumn)
    val missing = required.filterNot(indexByColumn.contains)

    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        "Input schema is missing required Task 1-1 column(s): " + missing.mkString(", ") +
          ". This task assumes ASR columns Date, Status, Qty, Size, and ship-state."
      )
    }

    RequiredColumns(
      date = indexByColumn(DateColumn),
      status = indexByColumn(StatusColumn),
      qty = indexByColumn(QtyColumn),
      size = indexByColumn(SizeColumn),
      state = indexByColumn(StateColumn)
    )
  }

  private def field(fields: Array[String], index: Int): String = {
    if (index >= 0 && index < fields.length) Option(fields(index)).getOrElse("") else ""
  }

  private def csvEscape(value: String): String = {
    val safe = Option(value).getOrElse("")
    if (safe.exists(ch => ch == ',' || ch == '"' || ch == '\n' || ch == '\r')) {
      "\"" + safe.replace("\"", "\"\"") + "\""
    } else {
      safe
    }
  }

  /**
   * Write one normal CSV file. Spark normally writes a folder containing part
   * files, so this method writes to a temporary folder and renames the single
   * part file to the requested output path.
   */
  private def writeSingleCsv(lines: RDD[String], outputFile: String): Unit = {
    val sc = lines.sparkContext
    val absOutputFile = "file://" + new java.io.File(outputFile).getAbsolutePath
    val outputPath = new Path(absOutputFile)
    val tempPath = new Path(absOutputFile + ".spark-tmp")
    val fs = tempPath.getFileSystem(sc.hadoopConfiguration)

    if (fs.exists(tempPath)) fs.delete(tempPath, true)
    if (fs.exists(outputPath)) fs.delete(outputPath, true)

    lines.coalesce(1).saveAsTextFile(tempPath.toString)

    val partFile = fs.listStatus(tempPath)
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .getOrElse {
        throw new IllegalStateException(s"Spark did not create a part file under $tempPath")
      }

    fs.rename(partFile, outputPath)
    fs.delete(tempPath, true)

    val parent = Option(outputPath.getParent).getOrElse(new Path("."))
    val checksumPath = new Path(parent, "." + outputPath.getName + ".crc")
    if (fs.exists(checksumPath)) fs.delete(checksumPath, false)
  }

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.nonEmpty) args(0) else "asr.csv"
    val outputPath = if (args.length >= 2) args(1) else "Task_1-1.csv"

    val spark = SparkSession.builder()
      .appName("Task 1-1: Most Bought Size by State (Sliding Window MapReduce)")
      .master("local[*]")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val absInput = "file://" + new java.io.File(inputPath).getAbsolutePath
      val lines = sc.textFile(absInput)
      val headerStr = lines.first()
      val header = parseCsvLine(headerStr)
      val columns = requireTaskColumns(header)
      val broadcastColumns = sc.broadcast(columns)

      val dataLines = lines.mapPartitionsWithIndex { case (partitionIndex, iterator) =>
        if (partitionIndex == 0) iterator.drop(1) else iterator
      }

      val parsedRows = dataLines.mapPartitions { iterator =>
        val col = broadcastColumns.value

        iterator.flatMap { line =>
          val fields = parseCsvLine(line)
          if (fields.length == 0) {
            Iterator.empty
          } else {
            val state = field(fields, col.state).trim
            val size = field(fields, col.size).trim
            val status = field(fields, col.status).toLowerCase(Locale.ROOT)

            parseDate(field(fields, col.date)).map { date =>
              (date.toEpochDay, state, size, status, field(fields, col.qty))
            }
          }
        }
      }

      val allEpochDays = parsedRows.map(_._1)
      val minEpochDay = allEpochDays.min()
      val maxEpochDay = allEpochDays.max()

      val purchases = parsedRows.flatMap { case (epochDay, state, size, status, qtyText) =>
        parsePositiveInt(qtyText).filter(_ => status.contains("shipped")).flatMap { qty =>
          if (state.nonEmpty && size.nonEmpty) Some(Purchase(epochDay, state, size, qty)) else None
        }
      }

      val windowContributions = purchases.flatMap { purchase =>
        (1L to 7L).iterator.flatMap { offset =>
          val targetDay = purchase.orderEpochDay + offset
          if (targetDay >= minEpochDay && targetDay <= maxEpochDay) {
            Some(((purchase.state, targetDay, purchase.size), purchase.qty))
          } else {
            None
          }
        }
      }

      val sizeTotals = windowContributions.reduceByKey(_ + _)

      val winners = sizeTotals
        .map { case ((state, day, size), totalQty) => ((state, day), (size, totalQty)) }
        .reduceByKey { (left: (String, Int), right: (String, Int)) =>
          val (leftSize, leftQty) = left
          val (rightSize, rightQty) = right

          if (leftQty > rightQty) left
          else if (rightQty > leftQty) right
          else if (leftSize <= rightSize) left
          else right
        }
        .sortBy { case ((state, day), _) => (state, day) }

      val csvRows = winners.map { case ((state, day), (size, totalQty)) =>
        val OutputDateFormatter = DateTimeFormatter.ofPattern("MM-dd-yy")
        val date = LocalDate.ofEpochDay(day).format(OutputDateFormatter)
        Seq(csvEscape(state), date, csvEscape(size), totalQty.toString).mkString(",")
      }

      val outputLines = sc.parallelize(Seq(OutputHeader), 1).union(csvRows)
      writeSingleCsv(outputLines, outputPath)

      println(s"Task 1-1 completed. Output written to $outputPath")
    } finally {
      spark.stop()
    }
  }
}