import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
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
object Task1_1 {

  private val DateColumn = "Date"
  private val StatusColumn = "Status"
  private val QtyColumn = "Qty"
  private val SizeColumn = "Size"
  private val StateColumn = "ship-state"

  private val OutputHeader = "ship-state,date,most_bought_size,max_quantity"

  private val DateFormatters = Seq(
    DateTimeFormatter.ofPattern("MM-dd-yy"),
    DateTimeFormatter.ofPattern("M-d-yy"),
    DateTimeFormatter.ofPattern("MM/dd/yy"),
    DateTimeFormatter.ofPattern("M/d/yy")
  )
  private val OutputDateFormatter = DateTimeFormatter.ofPattern("MM-dd-yy")

  private case class RequiredColumns(date: Int, status: Int, qty: Int, size: Int, state: Int)
  private case class Purchase(orderEpochDay: Long, state: String, size: String, qty: Int)

  private def newCsvParser(): CsvParser = {
    val settings = new CsvParserSettings()
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setIgnoreLeadingWhitespaces(false)
    settings.setIgnoreTrailingWhitespaces(false)
    new CsvParser(settings)
  }

  private def parseCsvLine(line: String): Array[String] = {
    val parsed = newCsvParser().parseLine(line)
    if (parsed == null) Array.empty[String] else parsed
  }

  private def parseDate(value: String): Option[LocalDate] = {
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
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(outputFile)
    val tempPath = new Path(outputFile + ".spark-tmp")

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

    // Hadoop LocalFileSystem can create a hidden checksum sidecar. The lab asks
    // for one readable CSV file, so remove the checksum if it appears.
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
      // Keep all input/output paths on the normal local filesystem even when a
      // lab machine has HDFS configured as Hadoop's default filesystem.
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val lines = sc.textFile(inputPath)
      val header = parseCsvLine(lines.first())
      val columns = requireTaskColumns(header)
      val broadcastColumns = sc.broadcast(columns)

      // Drop the first physical line only. The remaining RDD is the raw dataset
      // processed in MapReduce style.
      val dataLines = lines.mapPartitionsWithIndex { case (partitionIndex, iterator) =>
        if (partitionIndex == 0) iterator.drop(1) else iterator
      }

      val parsedRows = dataLines.mapPartitions { iterator =>
        val parser = newCsvParser()
        val col = broadcastColumns.value

        iterator.flatMap { line =>
          val fields = parser.parseLine(line)
          if (fields == null) {
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

      // The sliding window advances one calendar day at a time over the whole
      // dataset date span. This can create output dates that did not appear as
      // order dates, which matches the official PDF note about unseen timestamps.
      val allEpochDays = parsedRows.map(_._1)
      val minEpochDay = allEpochDays.min()
      val maxEpochDay = allEpochDays.max()

      // MAP 1: keep only rows that are true purchases for this query.
      val purchases = parsedRows.flatMap { case (epochDay, state, size, status, qtyText) =>
        parsePositiveInt(qtyText).filter(_ => status.contains("shipped")).flatMap { qty =>
          if (state.nonEmpty && size.nonEmpty) Some(Purchase(epochDay, state, size, qty)) else None
        }
      }

      // MAP 2: an order on day t contributes to target days t+1 through t+7.
      // For each emitted key, the target day d will count this order in [d-7,d-1].
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

      // REDUCE 1: total quantity per (state, sliding day, size).
      val sizeTotals = windowContributions.reduceByKey(_ + _)

      // MAP 3 + REDUCE 2: choose the size with the maximum quantity per
      // (state, sliding day). Ties are resolved lexicographically by size to make
      // the output deterministic across repeated runs.
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
