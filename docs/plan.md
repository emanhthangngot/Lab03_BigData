# Phân Công Nhiệm Vụ — Lab 03: Advanced MapReduce & Spark Structured APIs

**Môn:** Introduction to Big Data Analysis  
**Ngôn ngữ:** Scala (full điểm)  
**Bắt đầu từ đầu**

---

## Bảng phân công tổng quát

| Ký hiệu | MSSV | Họ tên | Task | Đầu ra |
|:---:|---|---|:---:|---|
| **A** | 23120099 | Lê Xuân Trí | 1-1 | `Task_1-1.csv` |
| **B** | 23120180 | Nguyễn Lê Trung Trực | 1-2 | `Task_1-2.csv` |
| **C** | 23120166 | Trần Hữu Kim Thành | 2-1 | `Task_2-1.parquet` |
| **D** | 23120185 | Nguyễn Hồ Anh Tuấn | 2-2 | `Task_2-2.parquet` |

---

## Chi tiết trách nhiệm

### A — Lê Xuân Trí (23120099)
| Hạng mục | Nội dung chi tiết |
|---|---|
| **Task** | **1-1: Sliding Window MapReduce** |
| **Lập trình** | Xây dựng pipeline MapReduce bằng Scala để tính size được mua nhiều nhất trong window 7 ngày. |
| **Dữ liệu ra** | File `Task_1-1.csv` (Single CSV, readable on normal filesystem). |
| **Báo cáo** | Phân tích query, mô tả quá trình phân tách (decomposition) và chiến lược thực thi. |
| **Tài liệu** | Code well-documented và README hướng dẫn chạy nếu môi trường phức tạp. |

### B — Nguyễn Lê Trung Trực (23120180)
| Hạng mục | Nội dung chi tiết |
|---|---|
| **Task** | **1-2: Median Variety MapReduce** |
| **Lập trình** | Xây dựng pipeline MapReduce bằng Scala để tính median variety cấp state theo từng tháng. |
| **Dữ liệu ra** | File `Task_1-2.csv` (Single CSV, readable on normal filesystem). |
| **Báo cáo** | Phân tích query, giải thích logic lọc size ≥ XXL và cách tính median trong MapReduce. |
| **Tài liệu** | Code well-documented và README hướng dẫn chạy nếu môi trường phức tạp. |

### C — Trần Hữu Kim Thành (23120166)
| Hạng mục | Nội dung chi tiết |
|---|---|
| **Task** | **2-1: Cancelled Orders (Spark Structured API)** |
| **Lập trình** | Sử dụng DataFrame/Dataset API (Scala) để tính % đơn hủy thỏa mãn 3 điều kiện phức hợp. |
| **Dữ liệu ra** | File `Task_2-1.parquet` (Single Parquet, parseable by Pandas/Spark). |
| **Báo cáo** | Phân tích query, decomposition. **Bắt buộc:** explain(true), phân tích join strategy, shuffle exchanges và stages. |
| **Tài liệu** | Code well-documented và README hướng dẫn chạy nếu môi trường phức tạp. |

### D — Nguyễn Hồ Anh Tuấn (23120185)
| Hạng mục | Nội dung chi tiết |
|---|---|
| **Task** | **2-2: Percentile StdDev (Spark Structured API)** |
| **Lập trình** | Thực hiện 2 cách tính percentile (built-in và self-implemented) để tính StdDev của Amount. |
| **Dữ liệu ra** | File `Task_2-2.parquet` (Single Parquet, parseable by Pandas/Spark). |
| **Báo cáo** | So sánh 2 phương pháp về độ chính xác, thời gian chạy (benchmark 5 lần) và các SKU-month group khác biệt. |
| **Tài liệu** | Code well-documented và README hướng dẫn chạy nếu môi trường phức tạp. |

---

## Kế hoạch báo cáo và nộp bài

### Cấu trúc Report.pdf
- Mỗi thành viên chịu trách nhiệm viết phần phân tích và kết quả cho Task của mình.
- Tổng hợp thành một file duy nhất với cấu trúc: Giới thiệu -> Task 1-1 -> Task 1-2 -> Task 2-1 -> Task 2-2 -> Kết luận.

### Quy định về nộp bài
- Toàn bộ source code được nén vào thư mục `<RepresentativeID>`.
- File `drive_link.txt` chứa link duy nhất đến Google Drive chứa các file dữ liệu đầu ra (`.csv` và `.parquet`).
- File nén cuối cùng là `<RepresentativeID>.zip`.

---

## Quy định và Lưu ý quan trọng

### 1. Ngôn ngữ và Môi trường
- Ưu tiên sử dụng Scala cho tất cả các bài tập để nhận tối đa điểm ngôn ngữ.
- Chỉ sử dụng native libraries nếu API của Spark không đủ đáp ứng yêu cầu.
- Sử dụng môi trường đã cài đặt từ Lab 1. Tuyệt đối không sử dụng Google Colab.

### 2. Kỹ thuật thực hiện
- Đối với MapReduce trong Python (nếu có): Không sử dụng các phương pháp streaming-based.
- Đối với Spark Structured APIs: Chỉ sử dụng DataFrame/Dataset API, không chấp nhận Spark SQL string queries trực tiếp để chấm điểm.
- Benchmark: Phải thực hiện ít nhất 5 lần cho mỗi phép đo, báo cáo giá trị trung bình (mean) và độ lệch chuẩn (standard deviation).

### 3. Cấu trúc thư mục Google Drive
Link trong `drive_link.txt` dẫn tới thư mục có cấu trúc:
- `<RepresentativeID>/Task_1-1.csv`
- `<RepresentativeID>/Task_1-2.csv`
- `<RepresentativeID>/Task_2-1.parquet`
- `<RepresentativeID>/Task_2-2.parquet`

### 4. Quy định về chỉnh sửa
Mọi chỉnh sửa sau thời hạn nộp bài trên Moodle đều sẽ dẫn đến việc kết quả của nhóm bị vô hiệu hóa.

### 5. Chất lượng Code
Code phải sạch, được comment rõ ràng để giải thích các bước logic phức tạp, phục vụ cho việc chấm điểm và bảo trì.
