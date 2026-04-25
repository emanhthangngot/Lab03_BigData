# Lab 02: Advanced MapReduce & Spark Structured APIs

**Môn:** Nhập môn Phân tích Dữ liệu Lớn (Introduction to Big Data Analysis)
**Giảng viên:** 
- TS. Nguyễn Ngọc Thảo | nnthao@fit.hcmus.edu.vn
- TS. Lê Ngọc Thanh | lnthanh@fit.hcmus.edu.vn

**Trợ giảng:**
- Trần Huy Bân | huyban.han@gmail.com
- Huỳnh Lâm Hải Đăng | hlhdang@fit.hcmus.edu.vn

---

## 1. Mở đầu (Preliminaries)

Để thực hiện bài lab này, bạn cần nắm vững các khái niệm cơ bản về Spark và Structured APIs. Bạn có thể đọc thêm tài liệu cơ bản tại các liên kết được cung cấp trong file PDF gốc.

### 1.1. Lịch sử của Spark
- Trong những ngày đầu, Spark thực hiện các thao tác dữ liệu trực tiếp trên RDD, việc này khó viết và khó tối ưu hóa hơn.
- Với sự ra đời của DataFrame và Structured APIs, việc viết các job Spark đã trở nên dễ dàng và hiệu quả hơn nhiều nhờ các tính năng tối ưu hóa tự động.
- Để hiểu đầy đủ các khái niệm như Catalyst optimizer, Whole-stage Code Gen, v.v., bạn nên đọc cuốn "Spark: The Definitive Guide".
- Các phiên bản sau này (từ 3.0) giới thiệu thêm Adaptive Query Execution (AQE) giúp tối ưu hóa job chạy nhanh hơn đáng kể.

### 1.2. DataFrame
DataFrame trong Spark có các đặc điểm:
- **Dựa trên Schema (Schema-based):** Có cấu trúc rõ ràng với tên cột và kiểu dữ liệu.
- **Bất biến (Immutable):** Không thể thay đổi sau khi tạo, chỉ có thể biến đổi thành DataFrame mới qua các phép transformation.
- **Đánh giá lười biếng (Lazy evaluation):** Chỉ thực hiện tính toán khi có một action được kích hoạt. Catalyst optimizer sẽ tối ưu hóa toàn bộ quá trình trước khi thực thi.

### 1.3. Bản chất của Spark so với MapReduce
- **MapReduce:** Mô hình dựa trên đĩa (disk-based), xử lý theo lô. Kết quả trung gian được ghi xuống đĩa giữa các pha map và reduce, dẫn đến tốc độ chậm do I/O thường xuyên.
- **Spark DataFrames:** Tận dụng xử lý trong bộ nhớ (in-memory), giảm thiểu I/O đĩa và cải thiện tốc độ. Cung cấp API cấp cao giống SQL, dễ đọc và súc tích hơn.
- **Tối ưu hóa:** Spark hưởng lợi từ Catalyst Optimizer và Tungsten Engine cho việc tối ưu hóa truy vấn tự động và thực thi hiệu quả bộ nhớ.

---

## 2. Mô tả bài tập (Problem Statements)

**Quy định chung:**
- Ngôn ngữ: Java, Scala, hoặc Python. **Chỉ các giải pháp viết bằng Scala mới nhận được điểm tối đa** về phần ngôn ngữ lập trình.
- Chỉ được dùng thư viện bản địa (như Numpy) nếu API của Spark không đủ đáp ứng.
- Dataset: `Amazon Sale Report` (asr.csv).
- Môi trường: Đã cài đặt từ Lab 1. **Không cho phép dùng Google Colab.**
- Benchmark: Chạy ít nhất 5 lần, lấy trung bình, báo cáo giá trị trung bình và độ lệch chuẩn.

### 2.1. Các bài tập Advanced MapReduce

1. **Sliding Window - Size được mua nhiều nhất theo bang:**
   - Xác định kích cỡ (size) được mua nhiều nhất tại mỗi bang trong khoảng thời gian tối đa 7 ngày trước ngày hiện tại (từ d-7 đến d-1).
   - Một mặt hàng được coi là "đã mua" nếu trạng thái đơn hàng là "shipped" và số lượng khác 0.
   - Cửa sổ (window) trượt mỗi ngày 1 lần.
   - Xuất kết quả ra file CSV duy nhất.
   - *Lưu ý:* Không dùng phương pháp streaming-based nếu dùng Python.

2. **Median Variety của Style:**
   - "Variety" (sự đa dạng) của một style là số lượng SKU khác nhau gắn với style đó trong một khoảng thời gian và khu vực địa lý cụ thể.
   - Tính **median variety** (trung vị của sự đa dạng) cấp bang cho mỗi tháng (VD: từ 07-01 đến 07-31).
   - Chỉ xét các style đã phục vụ kích cỡ **từ XXL trở lên** (XXL, 3XL, 4XL, v.v.).
   - Xuất kết quả ra file CSV duy nhất.

**Yêu cầu báo cáo:** Phân tích chi tiết các truy vấn, cách hiểu, cách phân rã bài toán và chiến lược thực hiện.

### 2.2. Các bài tập Structured APIs (Sử dụng Spark DataFrame/Dataset)

1. **Phần trăm đơn hàng bị hủy theo thành phố:**
   - Tính tỷ lệ (%) đơn hàng bị hủy có mức dịch vụ "Standard", sở hữu ít nhất 3 khuyến mãi (promotion) hợp lệ về mặt thời gian, và có số tiền mua thấp hơn số tiền trung bình của các đơn hàng "Merchant-fulfillment" có trạng thái "Shipped" trong cùng bang đó.
   - **Khuyến mãi hợp lệ về thời gian:** Có thời gian hoạt động ít nhất 2 ngày (tính từ lần xuất hiện đầu tiên đến lần xuất hiện cuối cùng của ID khuyến mãi đó trên toàn bộ dataset).
   - **Yêu cầu kỹ thuật:**
     - Chỉ dùng DataFrame/Dataset API. Không dùng truy vấn chuỗi Spark SQL trực tiếp.
     - Bao gồm output của `explain(true)` trong báo cáo.
     - Phân tích chiến lược join vật lý (Physical join strategy), số lượng shuffle exchanges (Exchange nodes) và số lượng stages.

2. **Độ lệch chuẩn của số tiền đơn hàng theo SKU-Tháng:**
   - Tính độ lệch chuẩn (standard deviation) của số tiền các đơn hàng có số lượng khuyến mãi đạt ngưỡng percentile động.
   - Hai mức ngưỡng: P90 (90th percentile) và P80 (80th percentile).
   - Số lượng khuyến mãi của đơn hàng được tính bằng cách đếm tất cả ID khuyến mãi liên quan (bao gồm cả khuyến mãi từ Amazon).
   - Nếu một nhóm SKU-Tháng có ít hơn 2 đơn hàng đủ điều kiện sau khi lọc, độ lệch chuẩn được đặt bằng 0.
   - **Yêu cầu thực hiện 2 phương pháp:**
     - Dùng hàm có sẵn `approx_percentile` của Spark.
     - Tự cài đặt tính percentile chính xác bằng các thao tác DataFrame/Dataset.
   - **Yêu cầu báo cáo:** So sánh 2 phương pháp về (a) độ chính xác, (b) thời gian thực thi, (c) phân tích các nhóm SKU-Tháng có kết quả khác nhau. Thảo luận về chiến lược repartitioning nếu nhóm có hơn 1,000 đơn hàng.

Xuất kết quả ra file PARQUET duy nhất có thể đọc được bằng Pandas hoặc Spark ở chế độ local.

---

## 3. Hướng dẫn nộp bài (Submission Guideline)

- Nộp theo nhóm, nén vào một file duy nhất. Một đại diện nộp trên Moodle.
- Giải pháp phải là một khối thống nhất (unified solution).
- Cấu trúc thư mục:
  ```
  <RepresentativeID>
  |--- src
  |    |--- Task_1-1
  |    |--- Task_1-2
  |    |--- Task_2-1
  |    |--- Task_2-2
  |--- docs
  |    |--- Report.pdf
  |    |--- drive_link.txt
  |    |--- README (tùy chọn)
  ```
- File `drive_link.txt` chứa link Google Drive dẫn tới các file kết quả:
  ```
  <RepresentativeID>
  |--- Task_1-1.csv
  |--- Task_1-2.csv
  |--- Task_2-1.parquet
  |--- Task_2-2.parquet
  ```

---

## Tiêu chí chấm điểm (Grading Criteria)

Mỗi bài tập đóng góp 2.5 điểm (Tổng 10 điểm).

| Yêu cầu cho mỗi bài tập | Điểm |
| :--- | :--- |
| Phân tích đúng các truy vấn | 0.5 |
| Phân rã thành công thành các bước cơ bản | 0.5 |
| Giải thích lý do đằng sau việc phân rã | 0.5 |
| Thực hiện thành công các bước trên | 0.5 |
| Đã kiểm thử và xuất kết quả thành công | 0.2 |
| Tính chính xác của kết quả xuất ra | 0.175 |
| Giải pháp viết bằng Scala (có thể chạy được) | 0.125 |
| **TỔNG CỘNG** | **2.5** |

**Lưu ý cuối cùng:**
- Đảm bảo mã nguồn được chú thích rõ ràng.
- Tuân thủ nghiêm ngặt cấu trúc file đã quy định.
- Tên file nén: `<RepresentativeID>.zip`.
