# Lab 3 - Advanced MapReduce and Spark Structured APIs

## 1. Requirements

The source code is implemented in Scala and executed with Apache Spark in local mode.

Required environment:

- Java 8, Java 11, or Java 17
- Apache Spark 3.x
- `spark-shell` available from the command line

The input dataset must be available at:

```text
data/input/Amazon Sale Report.csv
```

The commands below should be executed from the parent directory that contains both the submission folder and the `data/` directory:

```text
.
|-- 23120099/
|   |-- src/
|   |-- docs/
|-- data/
|   |-- input/
|   |   |-- Amazon Sale Report.csv
|   |-- output/
```

Create the output directory if it does not already exist:

```bash
mkdir -p data/output
```

## 2. Running Each Task

Each task is provided as a standalone Scala source file. The following commands create a temporary Spark runner for each task, load the corresponding source file, execute its `main` method, and write the required output file.

### Task 1-1

```bash
cat > /tmp/task1_1.runner.scala <<'EOF'
:load 23120099/src/Task_1-1/source/Task1_1.scala
Task1_1.main(Array(
  "data/input/Amazon Sale Report.csv",
  "data/output/Task_1-1.csv"
))
sys.exit(0)
EOF

SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost \
  spark-shell < /tmp/task1_1.runner.scala
```

### Task 1-2

```bash
cat > /tmp/task1_2.runner.scala <<'EOF'
:load 23120099/src/Task_1-2/source/Task1_2.scala
Task1_2.main(Array(
  "data/input/Amazon Sale Report.csv",
  "data/output/Task_1-2.csv"
))
sys.exit(0)
EOF

SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost \
  spark-shell < /tmp/task1_2.runner.scala
```

### Task 2-1

```bash
cat > /tmp/task2_1.runner.scala <<'EOF'
:load 23120099/src/Task_2-1/source/Task2_1.scala
Task2_1.main(Array(
  "data/input/Amazon Sale Report.csv",
  "data/output/Task_2-1.parquet"
))
sys.exit(0)
EOF

SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost \
  spark-shell < /tmp/task2_1.runner.scala
```

### Task 2-2

```bash
cat > /tmp/task2_2.runner.scala <<'EOF'
:load 23120099/src/Task_2-2/source/Task2_2.scala
Task2_2.main(Array(
  "data/input/Amazon Sale Report.csv",
  "data/output/Task_2-2.parquet"
))
sys.exit(0)
EOF

SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost \
  spark-shell < /tmp/task2_2.runner.scala
```

## 3. Expected Outputs

After all tasks complete successfully, the following files should be generated:

```text
data/output/Task_1-1.csv
data/output/Task_1-2.csv
data/output/Task_2-1.parquet
data/output/Task_2-2.parquet
```

Expected result sizes for the provided dataset:

```text
Task_1-1.csv      3,544 lines including the header
Task_1-2.csv        129 lines including the header
Task_2-1.parquet  2,452 rows
Task_2-2.parquet 16,345 rows
```

## 4. Execution Notes

- The implementation uses local filesystem paths internally to avoid unintended dependency on HDFS configuration.
- CSV outputs are written as single regular files.
- Parquet outputs are written as single `.parquet` files readable by Spark or Pandas.
- Existing output files with the same names are overwritten by the corresponding task.
