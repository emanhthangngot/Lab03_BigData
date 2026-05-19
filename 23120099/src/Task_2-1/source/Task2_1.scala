import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File

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
 *   ship-city (String), cancelled_percentage (Double)
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
      val absInput = "file://" + new File(inputPath).getAbsolutePath
      val rawDf = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(absInput)

      // Parse the Date column (MM-dd-yy) and cast Amount to Double.
      // Keep a unique row identifier to avoid ambiguity after joins.
      val df = rawDf
        .withColumn("parsed_date", to_date(col("Date"), "MM-dd-yy"))
        .withColumn("Amount", col("Amount").cast(DoubleType))
        .withColumn("ship_state_norm", upper(trim(col("ship-state"))))
        .withColumn("ship_city_norm", upper(trim(col("ship-city"))))
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

      val validPromoCount = promoValidity.count()
      println(s"[Task 2-1] Found $validPromoCount temporally-valid promotions")

      // Step 2c: Count valid promotions per order with DataFrame operations.
      // This keeps the logic visible to Catalyst instead of hiding it inside a UDF.
      val orderPromos = df
        .filter(col("promotion-ids").isNotNull && trim(col("promotion-ids")) =!= "")
        .select(
          col("order_row_id"),
          explode(
            transform(
              split(col("promotion-ids"), ","),
              (promoId: org.apache.spark.sql.Column) => trim(promoId)
            )
          ).as("promo_id")
        )
        .filter(col("promo_id") =!= "")

      val validPromotionCounts = orderPromos
        .join(broadcast(promoValidity), Seq("promo_id"), "inner")
        .groupBy("order_row_id")
        .agg(countDistinct("promo_id").as("valid_promo_count"))

      // -----------------------------------------------------------------------
      // 3. APPLY CONDITION 1 & CONDITION 2
      // -----------------------------------------------------------------------
      // Condition 1: ship-service-level = "Standard"
      // Condition 2: valid_promo_count >= 3
      val standardOrders = df
        .filter(
          lower(trim(col("ship-service-level"))) === "standard"
        )

      val afterCond12 = standardOrders
        .join(validPromotionCounts, Seq("order_row_id"), "left")
        .withColumn("valid_promo_count", coalesce(col("valid_promo_count"), lit(0L)))
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
        .groupBy("ship_state_norm")
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
        Seq("ship_state_norm"),
        "inner"
      )

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
          col("ship_city_norm")
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
        .select(col("ship_city_norm").as("ship-city"), col("cancelled_percentage"))
        .orderBy("ship-city")

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
      val absOutputFile = "file://" + new java.io.File(outputPath).getAbsolutePath
      val finalPath = new Path(absOutputFile)
      val tempDir   = new Path(absOutputFile + ".spark-tmp")
      val fs = tempDir.getFileSystem(spark.sparkContext.hadoopConfiguration)

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
