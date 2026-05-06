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
object Task1_2 {

  /** Kiểm tra size có thuộc nhóm >= XXL không */
  def isAtLeastXXL(size: String): Boolean = {
    val s = size.trim.toUpperCase
    val xlSizes = Set("XXL", "2XL", "XXXL", "3XL", "4XL", "5XL", "6XL")
    xlSizes.contains(s) || s.matches("([2-9]|\\d{2,})XL")
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("MM-dd-yy")

  /** Parse date MM-dd-yy thành tháng YYYY-MM */
  def parseMonth(dateStr: String): Option[String] = {
    try {
      val date = LocalDate.parse(dateStr.trim, dateFormatter)
      Some(f"${date.getYear}-${date.getMonthValue}%02d")
    } catch {
      case _: Exception => None
    }
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
    val spark = SparkSession.builder()
      .appName("Task 1-2: State-level Median Variety per Month")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Bước 0: Đọc CSV
    val df = spark.read.option("header", "true").csv("Amazon Sale Report.csv")

    // Bước 1: MAP — Clean data + emit (State, Month, Style) -> (SKU, isXXL)
    val mapped = df.select("Date", "SKU", "Style", "Size", "ship-state")
      .na.drop()
      .rdd
      .flatMap(row => {
        val dateStr = row.getAs[String]("Date")
        val sku     = row.getAs[String]("SKU")
        val style   = row.getAs[String]("Style")
        val size    = row.getAs[String]("Size").trim.toUpperCase
        val state   = row.getAs[String]("ship-state").trim.toUpperCase

        parseMonth(dateStr).map(month =>
          ((state, month, style), (sku, isAtLeastXXL(size)))
        )
      })

    // Bước 2: REDUCE 1 — Tính variety (distinct SKU count) + hasXXL per style
    val styleVariety = mapped.aggregateByKey((scala.collection.mutable.Set.empty[String], false))(
      (acc, value) => {
        acc._1 += value._1
        (acc._1, acc._2 || value._2)
      },
      (acc1, acc2) => {
        acc1._1 ++= acc2._1
        (acc1._1, acc1._2 || acc2._2)
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
    val outputPath = "Task_1-2.csv"
    val results = medianVariety.sortBy(r => (r._1, r._2)).collect()

    val writer = new PrintWriter(new File(outputPath))
    try {
      writer.println("ship-state,month,median_variety")
      results.foreach { case (state, month, median) =>
        writer.println(s"$state,$month,$median")
      }
    } finally {
      writer.close()
    }

    println(s"Task 1-2 hoàn thành: ${results.length} dòng -> $outputPath")
    spark.stop()
  }
}
