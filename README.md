# Hướng Dẫn Chạy Lab 03 - Scala + Spark

Dự án này chứa mã nguồn cho 4 phần của Lab 03. Project không dùng SBT; các task được biên dịch bằng `scalac`, đóng gói tạm bằng `jar`, rồi chạy bằng `spark-submit` ở chế độ local.

## 1. Yêu Cầu Môi Trường

Cần cài sẵn:

- Java 17, khuyến nghị cho Spark 3.x trên máy hiện tại.
- Scala 2.12.x.
- Apache Spark, có lệnh `spark-submit`.
- Dataset Amazon Sale Report:

```bash
/home/pearspringmind/Studying/Big Data/Lab03/Lab03_BigData/data/input/Amazon Sale Report.csv
```

Kiểm tra môi trường:

```bash
java -version
scala -version
scalac -version
spark-submit --version
```

Nếu máy đang mặc định Java 21, nên ép riêng phiên terminal dùng Java 17 trước khi compile/chạy:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
```

## 2. Cấu Trúc Task

| Task | File nguồn | Main class | Input mặc định | Output |
|---|---|---|---|---|
| Task 1-1 | `src/Task_1-1/src/main/scala/Task1_1.scala` | `Task1_1` | Amazon Sale Report CSV | `data/output/Task_1-1.csv` |
| Task 1-2 | `src/Task_1-2/src/main/scala/Task1_2.scala` | `Task1_2` | Amazon Sale Report CSV | `data/output/Task_1-2.csv` |
| Task 2-1 | `src/Task_2-1/src/main/scala/Task2_1.scala` | `Task2_1` | Amazon Sale Report CSV | `data/output/Task_2-1.parquet` |
| Task 2-2 | `src/Task_2-2/src/main/scala/Task2_2.scala` | `Task2_2` | Amazon Sale Report CSV | `data/output/Task_2-2.parquet` |

Ghi chú: các task có thể nhận đường dẫn input/output qua tham số dòng lệnh nếu mã nguồn của task đó đã hỗ trợ. Task 1-1 hiện đã hỗ trợ 2 tham số: `inputPath outputPath`.

## 3. Chuẩn Bị Thư Mục

Chạy từ thư mục gốc project:

```bash
cd "/home/pearspringmind/Studying/Big Data/Lab03/Lab03_BigData"
mkdir -p data/input data/output
```

Đặt file dataset vào:

```bash
data/input/Amazon Sale Report.csv
```

## 4. Compile Một Task

Cú pháp chung:

```bash
TASK_NAME="task11"
SOURCE_FILE="src/Task_1-1/src/main/scala/Task1_1.scala"

rm -rf "/tmp/lab03-${TASK_NAME}-classes" "/tmp/lab03-${TASK_NAME}.jar"
mkdir -p "/tmp/lab03-${TASK_NAME}-classes"

scalac -classpath "/home/pearspringmind/opt/spark/jars/*" \
  -d "/tmp/lab03-${TASK_NAME}-classes" \
  "$SOURCE_FILE"

jar cf "/tmp/lab03-${TASK_NAME}.jar" -C "/tmp/lab03-${TASK_NAME}-classes" .
```

Đổi `TASK_NAME` và `SOURCE_FILE` tương ứng khi compile task khác.

## 5. Chạy Các Task

### Task 1-1

```bash
TASK_NAME="task11"
SOURCE_FILE="src/Task_1-1/src/main/scala/Task1_1.scala"

rm -rf "/tmp/lab03-${TASK_NAME}-classes" "/tmp/lab03-${TASK_NAME}.jar"
mkdir -p "/tmp/lab03-${TASK_NAME}-classes"
scalac -classpath "/home/pearspringmind/opt/spark/jars/*" \
  -d "/tmp/lab03-${TASK_NAME}-classes" \
  "$SOURCE_FILE"
jar cf "/tmp/lab03-${TASK_NAME}.jar" -C "/tmp/lab03-${TASK_NAME}-classes" .

spark-submit \
  --class Task1_1 \
  --master local[*] \
  "/tmp/lab03-${TASK_NAME}.jar" \
  "data/input/Amazon Sale Report.csv" \
  "data/output/Task_1-1.csv"
```

### Task 1-2

```bash
TASK_NAME="task12"
SOURCE_FILE="src/Task_1-2/src/main/scala/Task1_2.scala"

rm -rf "/tmp/lab03-${TASK_NAME}-classes" "/tmp/lab03-${TASK_NAME}.jar"
mkdir -p "/tmp/lab03-${TASK_NAME}-classes"
scalac -classpath "/home/pearspringmind/opt/spark/jars/*" \
  -d "/tmp/lab03-${TASK_NAME}-classes" \
  "$SOURCE_FILE"
jar cf "/tmp/lab03-${TASK_NAME}.jar" -C "/tmp/lab03-${TASK_NAME}-classes" .

spark-submit \
  --class Task1_2 \
  --master local[*] \
  "/tmp/lab03-${TASK_NAME}.jar"
```

### Task 2-1

```bash
TASK_NAME="task21"
SOURCE_FILE="src/Task_2-1/src/main/scala/Task2_1.scala"

rm -rf "/tmp/lab03-${TASK_NAME}-classes" "/tmp/lab03-${TASK_NAME}.jar"
mkdir -p "/tmp/lab03-${TASK_NAME}-classes"
scalac -classpath "/home/pearspringmind/opt/spark/jars/*" \
  -d "/tmp/lab03-${TASK_NAME}-classes" \
  "$SOURCE_FILE"
jar cf "/tmp/lab03-${TASK_NAME}.jar" -C "/tmp/lab03-${TASK_NAME}-classes" .

spark-submit \
  --class Task2_1 \
  --master local[*] \
  "/tmp/lab03-${TASK_NAME}.jar"
```

### Task 2-2

```bash
TASK_NAME="task22"
SOURCE_FILE="src/Task_2-2/src/main/scala/Task2_2.scala"

rm -rf "/tmp/lab03-${TASK_NAME}-classes" "/tmp/lab03-${TASK_NAME}.jar"
mkdir -p "/tmp/lab03-${TASK_NAME}-classes"
scalac -classpath "/home/pearspringmind/opt/spark/jars/*" \
  -d "/tmp/lab03-${TASK_NAME}-classes" \
  "$SOURCE_FILE"
jar cf "/tmp/lab03-${TASK_NAME}.jar" -C "/tmp/lab03-${TASK_NAME}-classes" .

spark-submit \
  --class Task2_2 \
  --master local[*] \
  "/tmp/lab03-${TASK_NAME}.jar"
```

## 6. Kiểm Tra Output

Liệt kê các file kết quả:

```bash
find data/output -maxdepth 1 -mindepth 1 -printf "%f\n" | sort
```

Kết quả cuối cùng cần có theo đề:

```text
Task_1-1.csv
Task_1-2.csv
Task_2-1.parquet
Task_2-2.parquet
```

Kiểm tra nhanh Task 1-1:

```bash
sed -n '1,10p' data/output/Task_1-1.csv
wc -l data/output/Task_1-1.csv
```

Header đúng của Task 1-1:

```csv
ship-state,date,most_bought_size,max_quantity
```

## 7. Ghi Chú Kỹ Thuật

- Task 1-1 thuộc nhóm Advanced MapReduce, nên mã nguồn dùng RDD/MapReduce-style: `mapPartitions`, `flatMap`, `reduceByKey`, `sortBy`, `saveAsTextFile`.
- Task 2-1 và Task 2-2 thuộc nhóm Spark Structured APIs, nên khi hoàn thiện cần dùng DataFrame/Dataset API và không dùng Spark SQL string query làm logic chính.
- Output theo yêu cầu phải là file đơn lẻ đọc được trên filesystem bình thường, không phải thư mục chứa nhiều `part-*`.
- Nếu `spark-submit` báo lỗi socket trong môi trường sandbox, hãy chạy lệnh trong terminal thật của máy. Spark local cần mở driver socket nội bộ để thực thi job.
