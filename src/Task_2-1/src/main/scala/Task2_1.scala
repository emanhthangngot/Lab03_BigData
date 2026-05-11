import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Task 2-1: Percentage of Cancelled Orders per City
 *
 * For each city, calculate the percentage of cancelled orders that satisfy
 * ALL THREE conditions simultaneously:
 *   1) ship-service-level = "Standard"
 *   2) The order has >= 3 temporally-valid promotions
 *   3) The order's Amount < avg Amount of Merchant-fulfillment + Shipped orders
 *      in the same state
 *
 * A promotion is "temporally valid" if its active period (last appearance date
 * minus first appearance date across the ENTIRE dataset) spans >= 2 days.
 *
 * This solution uses ONLY DataFrame/Dataset API — no Spark SQL string queries.
 *
 * Output: Single Parquet file with schema:
 *   ship-city (String), ship-state (String), cancelled_percentage (Double)
 *
 * @author Trần Hữu Kim Thành (23120166)
 */
object Task2_1 {

  def main(args: Array[String]): Unit = {
    // -------------------------------------------------------------------------
    // 0. SETUP: SparkSession & paths
    // -------------------------------------------------------------------------
    val inputPath  = if (args.nonEmpty)      args(0) else "data/input/Amazon Sale Report.csv"
    val outputPath = if (args.length >= 2)   args(1) else "data/output/Task_2-1.parquet"

    val spark = SparkSession.builder()
      .appName("Task 2-1: Percentage of Cancelled Orders per City")
      .master("local[*]")
      // Force local filesystem so Hadoop-configured lab machines do not redirect
      // to HDFS by default.
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    try {
      // -----------------------------------------------------------------------
      // 1. LOAD CSV & CAST TYPES
      // -----------------------------------------------------------------------
      // Read with header; inferSchema is disabled — we cast manually for control.
      val rawDf = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(inputPath)

      // Parse the Date column (MM-dd-yy) and cast Amount to Double.
      // Keep a unique row identifier to avoid ambiguity after joins.
      val df = rawDf
        .withColumn("parsed_date", to_date(col("Date"), "MM-dd-yy"))
        .withColumn("Amount", col("Amount").cast(DoubleType))
        .withColumn("order_row_id", monotonically_increasing_id())

      // Cache because we reuse df in multiple branches.
      df.cache()

      println(s"[Task 2-1] Loaded ${df.count()} rows from $inputPath")

      // -----------------------------------------------------------------------
      // 2. COMPUTE TEMPORALLY-VALID PROMOTIONS (global, across all orders)
      // -----------------------------------------------------------------------
      // Step 2a: Explode promotion-ids into individual rows.
      //   - promotion-ids is a comma-separated string
      //   - Trim whitespace from each ID
      //   - Drop rows where promotion-ids is null/empty
      val promoExploded = df
        .filter(col("promotion-ids").isNotNull && col("promotion-ids") =!= "")
        .select(
          col("parsed_date"),
          explode(
            // Split by comma, then trim each element
            transform(
              split(col("promotion-ids"), ","),
              (promoId: org.apache.spark.sql.Column) => trim(promoId)
            )
          ).as("promo_id")
        )
        // Filter out empty strings that may result from trailing commas
        .filter(col("promo_id") =!= "")

      // Step 2b: For each unique promo_id, compute first/last appearance dates
      //          and determine if the promotion is temporally valid.
      val promoValidity = promoExploded
        .groupBy("promo_id")
        .agg(
          min("parsed_date").as("first_date"),
          max("parsed_date").as("last_date")
        )
        .withColumn("active_days", datediff(col("last_date"), col("first_date")))
        .filter(col("active_days") >= 2)
        .select("promo_id")

      // Collect the set of valid promotion IDs and broadcast it.
      // This avoids a large shuffle join for the per-order filtering step.
      val validPromoSet: Set[String] = promoValidity.as[String].collect().toSet
      val broadcastValidPromos = spark.sparkContext.broadcast(validPromoSet)

      println(s"[Task 2-1] Found ${validPromoSet.size} temporally-valid promotions")

      // Step 2c: UDF to count how many valid promotions an order has.
      val countValidPromos = udf((promoIdsRaw: String) => {
        if (promoIdsRaw == null || promoIdsRaw.trim.isEmpty) {
          0
        } else {
          val validSet = broadcastValidPromos.value
          promoIdsRaw
            .split(",")
            .map(_.trim)
            .filter(_.nonEmpty)
            .count(id => validSet.contains(id))
        }
      })

      // -----------------------------------------------------------------------
      // 3. APPLY CONDITION 1 & CONDITION 2
      // -----------------------------------------------------------------------
      // Condition 1: ship-service-level = "Standard"
      // Condition 2: valid_promo_count >= 3
      val afterCond12 = df
        .filter(
          lower(trim(col("ship-service-level"))) === "standard"
        )
        .withColumn("valid_promo_count", countValidPromos(col("promotion-ids")))
        .filter(col("valid_promo_count") >= 3)

      println(s"[Task 2-1] After Cond 1+2: ${afterCond12.count()} orders")

      // -----------------------------------------------------------------------
      // 4. COMPUTE STATE-LEVEL AVERAGE AMOUNT (Merchant + Shipped)
      // -----------------------------------------------------------------------
      // This is the reference average for Condition 3.
      // "Fulfilment" = "Merchant" AND "Courier Status" = "Shipped"
      val stateAvgDf = df
        .filter(
          lower(trim(col("Fulfilment"))) === "merchant" &&
          lower(trim(col("Courier Status"))) === "shipped"
        )
        .filter(col("Amount").isNotNull)
        .groupBy("ship-state")
        .agg(
          avg("Amount").as("merchant_shipped_avg")
        )

      println("[Task 2-1] State-level avg amounts computed:")
      stateAvgDf.show(10, truncate = false)

      // -----------------------------------------------------------------------
      // 5. APPLY CONDITION 3: Amount < merchant_shipped_avg of same state
      // -----------------------------------------------------------------------
      // Join orders (after Cond 1+2) with state averages.
      val joinedDf = afterCond12.join(
        stateAvgDf,
        afterCond12("ship-state") === stateAvgDf("ship-state"),
        "inner"
      ).drop(stateAvgDf("ship-state"))  // Remove duplicate join column

      // Filter: order Amount must be strictly less than the state average
      val qualifiedDf = joinedDf
        .filter(col("Amount").isNotNull)
        .filter(col("Amount") < col("merchant_shipped_avg"))

      println(s"[Task 2-1] After all 3 conditions: ${qualifiedDf.count()} qualifying orders")

      // -----------------------------------------------------------------------
      // 6. COMPUTE CANCELLED PERCENTAGE PER CITY
      // -----------------------------------------------------------------------
      // "Cancelled" is determined by the Status column containing "Cancelled".
      val resultDf = qualifiedDf
        .groupBy(
          col("ship-city"),
          col("ship-state")
        )
        .agg(
          count("*").as("total_orders"),
          sum(
            when(lower(trim(col("Status"))) === "cancelled", 1).otherwise(0)
          ).as("cancelled_orders")
        )
        .withColumn(
          "cancelled_percentage",
          round(col("cancelled_orders") / col("total_orders") * 100.0, 4)
        )
        .select("ship-city", "ship-state", "cancelled_percentage")
        .orderBy("ship-state", "ship-city")

      println("[Task 2-1] Result preview:")
      resultDf.show(20, truncate = false)
      println(s"[Task 2-1] Total cities in result: ${resultDf.count()}")

      // -----------------------------------------------------------------------
      // 7. EXPLAIN (for report)
      // -----------------------------------------------------------------------
      // The extended plan is REQUIRED in the report. Print it to console so
      // we can capture it.
      println("=" * 80)
      println("EXECUTION PLAN — explain(true)")
      println("=" * 80)
      resultDf.explain(true)
      println("=" * 80)

      // -----------------------------------------------------------------------
      // 8. EXPORT SINGLE PARQUET FILE
      // -----------------------------------------------------------------------
      // Spark writes a directory of part files by default. We coalesce to 1
      // partition, write to a temp directory, then rename the single part file
      // to the requested output path.
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val finalPath = new Path(outputPath)
      val tempDir   = new Path(outputPath + ".spark-tmp")

      // Clean up any previous output
      if (fs.exists(tempDir))  fs.delete(tempDir, true)
      if (fs.exists(finalPath)) fs.delete(finalPath, true)

      resultDf.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(tempDir.toString)

      // Find the single part file and rename it to the final output path
      val partFile = fs.listStatus(tempDir)
        .map(_.getPath)
        .find(_.getName.endsWith(".parquet"))
        .getOrElse {
          throw new IllegalStateException(
            s"Spark did not create a .parquet part file under $tempDir"
          )
        }

      fs.rename(partFile, finalPath)
      fs.delete(tempDir, true)

      // Also remove any Hadoop checksum sidecar file
      val parent = Option(finalPath.getParent).getOrElse(new Path("."))
      val checksumPath = new Path(parent, "." + finalPath.getName + ".crc")
      if (fs.exists(checksumPath)) fs.delete(checksumPath, false)

      println(s"[Task 2-1] Output written to $outputPath")

      // Unpersist the cached DataFrame
      df.unpersist()

      println("[Task 2-1] Completed successfully.")

    } finally {
      spark.stop()
    }
  }
}
