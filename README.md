# Lab 03 - Advanced MapReduce & Spark Structured APIs

Dự án này chứa mã nguồn Scala cho 4 bài tập (Task 1-1, 1-2, 2-1, 2-2) của Lab 03.
Để đảm bảo dễ dàng kiểm thử, project sử dụng `spark-shell` để nạp và chạy trực tiếp các file Scala nguồn mà không cần `sbt`.
`spark-submit` không chạy trực tiếp các file `.scala` trong repo này nếu chưa biên dịch chúng thành class hoặc JAR trước.

## 1. Yêu Cầu Môi Trường
- **Java 8, 11 hoặc 17** (được hỗ trợ bởi Spark 3.x)
- **Apache Spark** (cần có lệnh `spark-shell` trong biến môi trường `$PATH`; chỉ cần `spark-submit` nếu bạn tự biên dịch class/JAR riêng)
- **Dataset:** File dữ liệu cần nằm tại `data/input/Amazon Sale Report.csv`.

---

## 2. Cách Chạy Cả 4 Bài Tập Tự Động (Khuyên Dùng)

Chúng tôi đã chuẩn bị sẵn một script có tên `run_all.scala`. Script này sẽ tự động nạp (load) lần lượt 4 file nguồn Scala, thực thi chúng và lưu kết quả vào thư mục `data/output/`.

### 🐧 Trên Linux Native, macOS, hoặc WSL
Mở terminal tại thư mục gốc của project và chạy lệnh sau:
```bash
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost spark-shell < run_all.scala
```
Lưu ý:
- Không dùng `spark-shell -i run_all.scala < /dev/null` cho file này. Với Spark 3.4, cách đó có thể khởi động REPL rồi thoát mà không thực thi `run_all.scala`.
- Trên WSL, hãy dùng Java và Spark bản Linux trong chính WSL, không dùng file `.exe` của Windows.

---

## 3. Cách Chạy Từng Bài Tập Độc Lập

Nếu bạn muốn test từng bài một thay vì chạy cả 4, hãy tạo một file runner tạm chứa lệnh REPL rồi truyền file đó vào `spark-shell`.

**Task 1-1:**
```bash
cat > /tmp/task1_1.runner.scala <<'EOF'
:load src/Task_1-1/source/Task1_1.scala
Task1_1.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_1-1.csv"))
sys.exit(0)
EOF
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost spark-shell < /tmp/task1_1.runner.scala
```

**Task 1-2:**
```bash
cat > /tmp/task1_2.runner.scala <<'EOF'
:load src/Task_1-2/source/Task1_2.scala
Task1_2.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_1-2.csv"))
sys.exit(0)
EOF
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost spark-shell < /tmp/task1_2.runner.scala
```

**Task 2-1:**
```bash
cat > /tmp/task2_1.runner.scala <<'EOF'
:load src/Task_2-1/source/Task2_1.scala
Task2_1.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_2-1.parquet"))
sys.exit(0)
EOF
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost spark-shell < /tmp/task2_1.runner.scala
```

**Task 2-2:**
```bash
cat > /tmp/task2_2.runner.scala <<'EOF'
:load src/Task_2-2/source/Task2_2.scala
Task2_2.main(Array("data/input/Amazon Sale Report.csv", "data/output/Task_2-2.parquet"))
sys.exit(0)
EOF
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost spark-shell < /tmp/task2_2.runner.scala
```

---

## 4. Kết Quả Đầu Ra (Output)

Sau khi chạy xong, tất cả kết quả sẽ được tạo tự động trong thư mục `data/output/`:
1. `Task_1-1.csv` (1 file duy nhất)
2. `Task_1-2.csv` (1 file duy nhất)
3. `Task_2-1.parquet` (1 file duy nhất)
4. `Task_2-2.parquet` (1 file duy nhất)

Các file này đều hoàn toàn tuân thủ **Output Schema** đã được thống nhất của nhóm. Mọi giá trị kiểu số thập phân (`Double`) đều đã được làm tròn chính xác 4 chữ số thập phân (`.4f`).
