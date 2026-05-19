import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io.{PrintWriter, File}

/**
 * Task 1-2: State-level Median Variety per Month
 *
 * Với mỗi (bang, tháng), tính trung vị (median) của "variety" trong tất cả
 * các styles có ít nhất 1 record với size >= XXL.
 * Variety = số lượng SKU khác biệt (distinct) gắn với 1 style.
 *
 * Pipeline: MAP → REDUCE 1 (variety per style) → MAP → REDUCE 2 (median)
 *
 * @author Trần Lê Trung Trực (23120180)
 */
object Task1_2 extends Serializable {

  case class RequiredColumns(date: Int, sku: Int, style: Int, size: Int, state: Int)

  private val DateColumn = "Date"
  private val SkuColumn = "SKU"
  private val StyleColumn = "Style"
  private val SizeColumn = "Size"
  private val StateColumn = "ship-state"

  /** Custom CSV parser that supports quoted fields and escaped quotes. */
  def parseCsvLine(line: String): Array[String] = {
    val fields = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var inQuotes = false
    var i = 0

    while (i < line.length) {
      val c = line.charAt(i)
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length && line.charAt(i + 1) == '"') {
            current.append('"')
            i += 1
          } else {
            inQuotes = false
          }
        } else {
          current.append(c)
        }
      } else {
        if (c == '"') inQuotes = true
        else if (c == ',') {
          fields += current.toString
          current.setLength(0)
        } else {
          current.append(c)
        }
      }
      i += 1
    }

    fields += current.toString
    fields.toArray
  }

  private def requireTaskColumns(header: Array[String]): RequiredColumns = {
    val indexByColumn = header.zipWithIndex.map { case (name, index) => name.trim -> index }.toMap
    val required = Seq(DateColumn, SkuColumn, StyleColumn, SizeColumn, StateColumn)
    val missing = required.filterNot(indexByColumn.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        "Input schema is missing required Task 1-2 column(s): " + missing.mkString(", ")
      )
    }

    RequiredColumns(
      date = indexByColumn(DateColumn),
      sku = indexByColumn(SkuColumn),
      style = indexByColumn(StyleColumn),
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

  /** Kiểm tra size có thuộc nhóm >= XXL không */
  def isAtLeastXXL(size: String): Boolean = {
    val s = size.trim.toUpperCase
    val xlSizes = Set("XXL", "2XL", "XXXL", "3XL", "4XL", "5XL", "6XL")
    xlSizes.contains(s) || s.matches("([2-9]|\\d{2,})XL")
  }

  /** Parse date MM-dd-yy thành tháng YYYY-MM */
  def parseMonth(dateStr: String): Option[String] = {
    val dateFormatters = Seq(
      DateTimeFormatter.ofPattern("MM-dd-yy"),
      DateTimeFormatter.ofPattern("M-d-yy"),
      DateTimeFormatter.ofPattern("MM/dd/yy"),
      DateTimeFormatter.ofPattern("M/d/yy")
    )
    dateFormatters.iterator
      .map(formatter => scala.util.Try(LocalDate.parse(dateStr.trim, formatter)).toOption)
      .collectFirst { case Some(date) => f"${date.getYear}-${date.getMonthValue}%02d" }
  }

  /** Tính median của danh sách đã sort, làm tròn 4 chữ số thập phân */
  def computeMedian(sortedValues: List[Int]): Option[String] = {
    if (sortedValues.isEmpty) None
    else {
      val n = sortedValues.size
      val median = if (n % 2 == 1) sortedValues(n / 2).toDouble
                   else (sortedValues(n / 2 - 1) + sortedValues(n / 2)) / 2.0
      Some(f"$median%.4f")
    }
  }

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.nonEmpty) args(0) else "Amazon Sale Report.csv"
    val outputPath = if (args.length >= 2) args(1) else "Task_1-2.csv"

    val spark = SparkSession.builder()
      .appName("Task 1-2: State-level Median Variety per Month")
      .master("local[*]")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Bước 0: Đọc CSV bằng RDD từ local filesystem, kể cả khi máy có HDFS mặc định.
    val absInput = "file://" + new File(inputPath).getAbsolutePath
    val lines = sc.textFile(absInput)
    val header = parseCsvLine(lines.first())
    val columns = requireTaskColumns(header)
    val broadcastColumns = sc.broadcast(columns)

    val dataLines = lines.mapPartitionsWithIndex { case (partitionIndex, iterator) =>
      if (partitionIndex == 0) iterator.drop(1) else iterator
    }

    // Bước 1: MAP — Clean data + emit (State, Month, Style) -> (SKU, isXXL)
    val mapped = dataLines.flatMap(line => {
        val col = broadcastColumns.value
        val fields = parseCsvLine(line)
        val dateStr = field(fields, col.date).trim
        val sku     = field(fields, col.sku).trim
        val style   = field(fields, col.style).trim
        val size    = field(fields, col.size).trim.toUpperCase
        val state   = field(fields, col.state).trim.toUpperCase

        if (dateStr.isEmpty || sku.isEmpty || style.isEmpty || size.isEmpty || state.isEmpty) {
          None
        } else {
          parseMonth(dateStr).map(month =>
            ((state, month, style), (sku, isAtLeastXXL(size)))
          )
        }
      })

    // Bước 2: REDUCE 1 — Tính variety (distinct SKU count) + hasXXL per style
    val styleVariety = mapped.aggregateByKey((Set.empty[String], false))(
      (acc, value) => {
        (acc._1 + value._1, acc._2 || value._2)
      },
      (acc1, acc2) => {
        (acc1._1 ++ acc2._1, acc1._2 || acc2._2)
      }
    ).map {
      case ((state, month, _), (skuSet, hasXXL)) =>
        ((state, month), (skuSet.size, hasXXL))
    }

    // Bước 3: REDUCE 2 — Lọc qualifying styles trước khi shuffle, tính median variety per (State, Month)
    val medianVariety = styleVariety
      .filter { case (_, (_, hasXXL)) => hasXXL }
      .map { case (k, (size, _)) => (k, size) }
      .groupByKey()
      .flatMap { case ((state, month), sizesIter) =>
        val qualifyingVarieties = sizesIter.toList.sorted

        computeMedian(qualifyingVarieties).map(median =>
          (state, month, median)
        )
      }

    // Bước 4: Export ra single CSV file
    val results = medianVariety.sortBy(r => (r._1, r._2)).collect()

    val writer = new PrintWriter(new File(outputPath))
    try {
      writer.println("ship-state,month,median_variety")
      results.foreach { case (state, month, median) =>
        writer.println(s"${csvEscape(state)},$month,$median")
      }
    } finally {
      writer.close()
    }

    println(s"Task 1-2 hoàn thành: ${results.length} dòng -> $outputPath")
    spark.stop()
  }
}
