# Lab 03 - Advanced MapReduce & Spark Structured APIs

Dự án này chứa mã nguồn Scala cho 4 bài tập (Task 1-1, 1-2, 2-1, 2-2) của Lab 03.
Để đảm bảo dễ dàng kiểm thử, project sử dụng công cụ `spark-shell` để tự động compile và chạy toàn bộ mã nguồn mà không cần cài đặt các công cụ build phức tạp như `sbt`.

## 1. Yêu Cầu Môi Trường
- **Java 8, 11 hoặc 17** (được hỗ trợ bởi Spark 3.x)
- **Apache Spark** (cần có lệnh `spark-shell` và `spark-submit` trong biến môi trường `$PATH`)
- **Dataset:** Cần đặt dataset `Amazon Sale Report.csv` vào thư mục gốc của project (nơi chứa file `README.md`).

---

## 2. Cách Chạy Cả 4 Bài Tập Tự Động (Khuyên Dùng)

Chúng tôi đã chuẩn bị sẵn một script có tên `run_all.scala`. Script này sẽ tự động nạp (load) lần lượt 4 file nguồn Scala, thực thi chúng và lưu kết quả vào thư mục `data/output/`.

### 🐧 Trên Linux Native hoặc macOS
Mở terminal tại thư mục gốc của project và chạy lệnh sau:
```bash
spark-shell < run_all.scala
```

### 🪟 Trên Windows Subsystem for Linux (WSL)
Trên WSL, việc pipe direct file vào spark-shell thỉnh thoảng gặp lỗi hiển thị/treo do JLine. Bạn nên chạy lệnh này thay thế:
```bash
spark-shell -i run_all.scala < /dev/null
```
*(Lưu ý: Môi trường WSL cần cài đặt Java và Spark cho Linux đúng cách, không dùng file .exe của Windows).*

---

## 3. Cách Chạy Từng Bài Tập Độc Lập

Nếu bạn muốn test từng bài một thay vì chạy cả 4, bạn có thể truyền thẳng code Scala vào spark-shell.

**Task 1-1:**
```bash
spark-shell -i <(echo ':load src/Task_1-1/src/main/scala/Task1_1.scala
Task1_1.main(Array("Amazon Sale Report.csv", "data/output/Task_1-1.csv"))
sys.exit(0)')
```

**Task 1-2:**
```bash
spark-shell -i <(echo ':load src/Task_1-2/src/main/scala/Task1_2.scala
Task1_2.main(Array("Amazon Sale Report.csv", "data/output/Task_1-2.csv"))
sys.exit(0)')
```

**Task 2-1:**
```bash
spark-shell -i <(echo ':load src/Task_2-1/src/main/scala/Task2_1.scala
Task2_1.main(Array("Amazon Sale Report.csv", "data/output/Task_2-1.parquet"))
sys.exit(0)')
```

**Task 2-2:**
```bash
spark-shell -i <(echo ':load src/Task_2-2/src/main/scala/Task2_2.scala
Task2_2.main(Array("Amazon Sale Report.csv", "data/output/Task_2-2.parquet"))
sys.exit(0)')
```

---

## 4. Kết Quả Đầu Ra (Output)

Sau khi chạy xong, tất cả kết quả sẽ được tạo tự động trong thư mục `data/output/`:
1. `Task_1-1.csv` (1 file duy nhất)
2. `Task_1-2.csv` (1 file duy nhất)
3. `Task_2-1.parquet` (1 file duy nhất)
4. `Task_2-2.parquet` (1 file duy nhất)

Các file này đều hoàn toàn tuân thủ **Output Schema** đã được thống nhất của nhóm. Mọi giá trị kiểu số thập phân (`Double`) đều đã được làm tròn chính xác 4 chữ số thập phân (`.4f`).
