import org.apache.spark.sql.SparkSession

object Task2_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task 2-1: Percentage of Cancelled Orders per City")
      .master("local[*]")
      .getOrCreate()

    // TODO: Implement Structured API logic here
    println("Task 2-1 started.")

    spark.stop()
  }
}
