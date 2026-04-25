import org.apache.spark.sql.SparkSession

object Task1_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task 1-2: State-level Median Variety per Month")
      .master("local[*]")
      .getOrCreate()

    // TODO: Implement Median Variety MapReduce / Spark logic here
    println("Task 1-2 started.")

    spark.stop()
  }
}
