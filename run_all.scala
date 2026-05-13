import java.io.File

val outDir = new File("data/output")
if (!outDir.exists()) outDir.mkdirs()

println("\n======================================")
println("Running Task 1-1")
println("======================================")
:load src/Task_1-1/source/Task1_1.scala
Task1_1.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_1-1.csv"))

println("\n======================================")
println("Running Task 1-2")
println("======================================")
:load src/Task_1-2/source/Task1_2.scala
Task1_2.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_1-2.csv"))

println("\n======================================")
println("Running Task 2-1")
println("======================================")
:load src/Task_2-1/source/Task2_1.scala
Task2_1.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_2-1.parquet"))

println("\n======================================")
println("Running Task 2-2")
println("======================================")
:load src/Task_2-2/source/Task2_2.scala
Task2_2.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_2-2.parquet"))

println("\n======================================")
println("ALL TASKS COMPLETED SUCCESSFULLY!")
println("Outputs are located in 'data/output/'")
println("======================================")
sys.exit(0)
