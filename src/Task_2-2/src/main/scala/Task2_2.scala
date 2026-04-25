import org.apache.spark.sql.SparkSession

object Task2_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task 2-2: StdDev of Amount by SKU-Month")
      .master("local[*]")
      .getOrCreate()

    // TODO: Implement Structured API logic here (P90 and P80)
    println("Task 2-2 started.")

    spark.stop()
  }
}
