import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Task 2-2: Standard Deviation of Order Amount by SKU-Month
 *           with Dynamic P90 / P80 Percentile Threshold
 *
 * Problem:
 *   For each (SKU, month) group, retain only orders whose promotion count
 *   is >= the P90 (or P80) percentile of promotion counts in that group,
 *   then compute the population standard deviation (ddof = 0) of their Amount.
 *   If fewer than 2 orders qualify after filtering, stddev is set to 0.
 *
 * Two approaches for percentile computation:
 *   Approach 1 — Built-in approx:  Spark's percentile_approx()
 *   Approach 2 — Self-implemented: exact percentile via window row_number()
 *
 * Output schema (Parquet, single file):
 *   SKU                (String)
 *   month              (String, format "YYYY-MM")
 *   p90_stddev_approx  (Double)
 *   p90_stddev_exact   (Double)
 *   p80_stddev_approx  (Double)
 *   p80_stddev_exact   (Double)
 *
 * Author : Nguyen Ho Anh Tuan (23120185) — Member D
 * Dataset: Amazon Sale Report (asr.csv)
 */
object Task2_2 {

  // --------------------------------------------------------------------------
  // Helper: compute population stddev for qualifying orders (count-aware)
  // Given a DataFrame already filtered to qualifying rows, group by (SKU, month)
  // and return stddev; groups with < 2 rows get 0.0.
  // --------------------------------------------------------------------------
  private def computeStddev(
    df: org.apache.spark.sql.DataFrame,
    colAlias: String
  ): org.apache.spark.sql.DataFrame = {
    df.groupBy("SKU", "month")
      .agg(
        count("*").alias("_cnt"),
        stddev_pop(col("amount_val")).alias("_raw_std")
      )
      .withColumn(
        colAlias,
        when(col("_cnt") < 2, lit(0.0))
          .otherwise(col("_raw_std"))
      )
      .select("SKU", "month", colAlias)
  }

  private def writeParquetOutput(
    df: org.apache.spark.sql.DataFrame,
    outputPath: String,
    spark: SparkSession
  ): Unit = {
    val outputIsFile = outputPath.toLowerCase.endsWith(".parquet")
    if (!outputIsFile) {
      df.coalesce(1).write.mode("overwrite").parquet(outputPath)
      return
    }

    val tmpDir = outputPath + "_tmp"
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val tmpPath = new Path(tmpDir)
    val outPath = new Path(outputPath)

    if (fs.exists(tmpPath)) fs.delete(tmpPath, true)
    if (fs.exists(outPath)) fs.delete(outPath, true)

    val parent = outPath.getParent
    if (parent != null && !fs.exists(parent)) {
      fs.mkdirs(parent)
    }

    df.coalesce(1).write.mode("overwrite").parquet(tmpDir)

    val partFile = fs.listStatus(tmpPath)
      .find { status =>
        val name = status.getPath.getName
        status.isFile && name.startsWith("part-") && name.endsWith(".parquet")
      }
      .getOrElse(throw new RuntimeException(s"No parquet part file found in $tmpDir"))
      .getPath

    if (!fs.rename(partFile, outPath)) {
      throw new RuntimeException(s"Failed to move $partFile to $outputPath")
    }

    fs.delete(tmpPath, true)
  }

  def main(args: Array[String]): Unit = {

    // -------------------------------------------------------------------------
    // 0. SparkSession
    // -------------------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Task 2-2: StdDev of Amount by SKU-Month with Percentile Threshold")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")    // tuned for local + moderate data
      .config("spark.sql.adaptive.enabled", "true")   // AQE for auto-optimisation
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Paths: override via CLI args if needed
    val inputPath  = if (args.length > 0) args(0)
                     else "data/input/Amazon Sale Report.csv"
    val outputPath = if (args.length > 1) args(1)
                     else "data/output/Task_2-2.parquet"

    println(s"[Task2-2] Input  : $inputPath")
    println(s"[Task2-2] Output : $outputPath")

    // =========================================================================
    // STEP 1 — Load raw CSV
    // =========================================================================
    val rawDf = spark.read
      .option("header",    "true")
      .option("inferSchema", "false")   // keep everything as String initially
      .option("quote",     "\"")
      .option("escape",    "\"")
      .csv(inputPath)

    // =========================================================================
    // STEP 2 — Select & clean relevant columns
    //
    // Required columns:
    //   SKU           — product identifier
    //   Date          — order date → extract month (YYYY-MM)
    //   promotion-ids — comma-separated promo IDs (may be null / empty)
    //   Amount        — order monetary value
    // =========================================================================
    val selectedDf = rawDf.select(
      trim(col("SKU")).alias("SKU"),
      trim(col("Date")).alias("raw_date"),
      col("promotion-ids"),
      col("Amount")
    )
    .filter(col("SKU").isNotNull      && col("SKU")      =!= "")
    .filter(col("raw_date").isNotNull && col("raw_date") =!= "")
    .withColumn("amount_val", col("Amount").cast(DoubleType))
    .filter(col("amount_val").isNotNull)

    // =========================================================================
    // STEP 3 — Parse Date → month string "YYYY-MM"
    //
    // Amazon Sale Report dates are typically "MM-DD-YY" (e.g. "04-04-22").
    // We cascade through common formats to handle any variation.
    // =========================================================================
    val withMonthDf = selectedDf
      .withColumn(
        "parsed_date",
        coalesce(
          to_date(col("raw_date"), "M/d/yyyy"),   
          to_date(col("raw_date"), "MM/dd/yyyy"),
          to_date(col("raw_date"), "MM-dd-yy"),
          to_date(col("raw_date"), "M-d-yy"),
          to_date(col("raw_date"), "MM/dd/yy"),
          to_date(col("raw_date"), "M/d/yy"),
          to_date(col("raw_date"), "dd-MM-yyyy"),
          to_date(col("raw_date"), "MM-dd-yyyy")
        )
      )
      .filter(col("parsed_date").isNotNull)
      .withColumn("month", date_format(col("parsed_date"), "yyyy-MM"))

    // =========================================================================
    // STEP 4 — Compute promotion count per order
    //
    // promotion-ids is a comma-separated list (e.g. "AAT-ABC, AAT-DEF").
    // Count = number of non-empty tokens after splitting on ",".
    // Null or blank promotion-ids → count = 0.
    // Amazon-issued promotions are included as per the problem statement.
    // =========================================================================
    val baseDf = withMonthDf
      .withColumn(
        "promo_count",
        when(
          col("promotion-ids").isNull || trim(col("promotion-ids")) === "",
          lit(0L)
        ).otherwise(
          // Split → trim whitespace per token → remove blanks → count
          size(
            filter(
              split(col("promotion-ids"), ","),
              (token: Column) => trim(token) =!= lit("")
            )
          ).cast(LongType)
        )
      )
      .select("SKU", "month", "promo_count", "amount_val")

    // Cache: reused by both approaches
    baseDf.cache()
    val totalRows = baseDf.count()
    println(s"[Task2-2] Parsed rows: $totalRows")

    // =========================================================================
    // APPROACH 1 — approx_percentile (Spark built-in, approximate)
    //
    // percentile_approx(col, p, accuracy) computes the p-th percentile with
    // Greenwald-Khanna algorithm (accuracy = 10000 → very low relative error).
    // This is fast (single-pass aggregate) but may differ slightly from exact.
    // =========================================================================
    println("[Task2-2] Running Approach 1: approx_percentile ...")

    // Compute thresholds per (SKU, month) group in one aggregation pass
    val approxThresholds = baseDf
      .groupBy("SKU", "month")
      .agg(
        percentile_approx(col("promo_count"), lit(0.9), lit(10000))
          .alias("p90_thresh"),
        percentile_approx(col("promo_count"), lit(0.8), lit(10000))
          .alias("p80_thresh")
      )

    // Join thresholds back to base, then filter & compute stddev
    val withApprox = baseDf.join(approxThresholds, Seq("SKU", "month"), "inner")

    val p90ApproxDf = computeStddev(
      withApprox.filter(col("promo_count") >= col("p90_thresh")),
      "p90_stddev_approx"
    )
    val p80ApproxDf = computeStddev(
      withApprox.filter(col("promo_count") >= col("p80_thresh")),
      "p80_stddev_approx"
    )

    // =========================================================================
    // APPROACH 2 — Self-implemented exact percentile
    //
    // Decomposition:
    //   a) Assign row_number() per (SKU, month) ordered by promo_count ASC
    //   b) Count total rows N per (SKU, month)
    //   c) Percentile position = CEIL(p * N)  [nearest-rank method]
    //   d) The threshold value = promo_count at that ranked row
    //   e) Filter orders where promo_count >= threshold, then compute stddev
    //
    // Rationale for nearest-rank (CEIL):
    //   The nearest-rank method guarantees that the percentile value is always
    //   an actual observed value in the data (no interpolation), which is
    //   consistent with how approx_percentile behaves and makes filtering
    //   with >= straightforward.
    // =========================================================================
    println("[Task2-2] Running Approach 2: exact percentile ...")

    // Window partitioned by (SKU, month), ordered ascending by promo_count
    val winGroup    = Window.partitionBy("SKU", "month")
    val winGroupOrd = Window.partitionBy("SKU", "month").orderBy(col("promo_count").asc)

    // Add rank (1-indexed) and group size
    val rankedDf = baseDf
      .withColumn("rn",      row_number().over(winGroupOrd))  // 1 … N
      .withColumn("total_n", count("*").over(winGroup))        // N

    // Compute target positions for P90 and P80
    val withPosDf = rankedDf
      .withColumn("pos_p90", ceil(lit(0.9) * col("total_n")).cast(LongType))
      .withColumn("pos_p80", ceil(lit(0.8) * col("total_n")).cast(LongType))

    // Extract the threshold value at each percentile position
    // We filter rows where rn == pos_pXX, then take distinct (SKU, month) → threshold
    val threshP90Exact = withPosDf
      .filter(col("rn") === col("pos_p90"))
      .select(col("SKU"), col("month"), col("promo_count").alias("p90_thresh_exact"))
      .dropDuplicates("SKU", "month")   // safety: same promo_count may appear at multiple ranks

    val threshP80Exact = withPosDf
      .filter(col("rn") === col("pos_p80"))
      .select(col("SKU"), col("month"), col("promo_count").alias("p80_thresh_exact"))
      .dropDuplicates("SKU", "month")

    // Join exact thresholds back to base data
    val withExact = baseDf
      .join(threshP90Exact, Seq("SKU", "month"), "inner")
      .join(threshP80Exact, Seq("SKU", "month"), "inner")

    val p90ExactDf = computeStddev(
      withExact.filter(col("promo_count") >= col("p90_thresh_exact")),
      "p90_stddev_exact"
    )
    val p80ExactDf = computeStddev(
      withExact.filter(col("promo_count") >= col("p80_thresh_exact")),
      "p80_stddev_exact"
    )

    // =========================================================================
    // STEP 5 — Assemble final result
    //
    // Use all (SKU, month) groups as the base (left join) so that groups
    // with no qualifying orders still appear with stddev = 0.0.
    // =========================================================================
    val allGroups = baseDf.select("SKU", "month").distinct()

    val resultDf = allGroups
      .join(p90ApproxDf, Seq("SKU", "month"), "left")
      .join(p80ApproxDf, Seq("SKU", "month"), "left")
      .join(p90ExactDf,  Seq("SKU", "month"), "left")
      .join(p80ExactDf,  Seq("SKU", "month"), "left")
      // Null → 0.0 for groups with 0 qualifying orders (shouldn't normally happen
      // since at least 1 order always meets promo_count >= threshold for nearest-rank)
      .withColumn("p90_stddev_approx", round(coalesce(col("p90_stddev_approx"), lit(0.0)), 4))
      .withColumn("p80_stddev_approx", round(coalesce(col("p80_stddev_approx"), lit(0.0)), 4))
      .withColumn("p90_stddev_exact",  round(coalesce(col("p90_stddev_exact"),  lit(0.0)), 4))
      .withColumn("p80_stddev_exact",  round(coalesce(col("p80_stddev_exact"),  lit(0.0)), 4))
      .select(
        col("SKU"),
        col("month"),
        col("p90_stddev_approx"),
        col("p90_stddev_exact"),
        col("p80_stddev_approx"),
        col("p80_stddev_exact")
      )
      .orderBy("SKU", "month")

    // =========================================================================
    // STEP 6 — Export to single Parquet file
    //
    // coalesce(1) forces a single output part file.
    // Output is readable by both Pandas (pd.read_parquet) and Spark local mode.
    // =========================================================================
    println("[Task2-2] Writing output ...")
    writeParquetOutput(resultDf, outputPath, spark)

    println(s"[Task2-2] Done. Total (SKU, month) groups: ${resultDf.count()}")
    println(s"[Task2-2] Output written to: $outputPath")

    // Show a small sample for quick sanity check
    println("[Task2-2] Sample output (top 20 rows):")
    resultDf.show(20, truncate = false)

    baseDf.unpersist()
    spark.stop()
  }
}
