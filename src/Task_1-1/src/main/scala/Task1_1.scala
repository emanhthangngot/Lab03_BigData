import org.apache.spark.sql.SparkSession

object Task1_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task 1-1: Most Bought Size by State (Sliding Window)")
      .master("local[*]")
      .getOrCreate()

    // TODO: Implement Sliding Window MapReduce / Spark logic here
    println("Task 1-1 started.")

    spark.stop()
  }
}
