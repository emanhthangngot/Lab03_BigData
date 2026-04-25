# Quy định Schema đầu ra (Output Schema)

Để đảm bảo tính nhất quán cho bài nộp cuối cùng, nhóm thống nhất cấu trúc các file đầu ra như sau:

## 1. Task 1-1 (MapReduce)
- **Tên file:** `Task_1-1.csv`
- **Định dạng:** CSV (có header)
- **Schema:**
  - `ship-state` (String): Tên bang/tỉnh.
  - `date` (String/Date): Ngày tính toán (ngày d).
  - `most_bought_size` (String): Kích cỡ được mua nhiều nhất trong khoảng [d-7, d-1].
  - `max_quantity` (Int): Số lượng của kích cỡ đó trong window (phục vụ đối soát).

## 2. Task 1-2 (MapReduce)
- **Tên file:** `Task_1-2.csv`
- **Định dạng:** CSV (có header)
- **Schema:**
  - `ship-state` (String): Tên bang/tỉnh.
  - `month` (String): Tháng (định dạng `MM-YYYY` hoặc `YYYY-MM`).
  - `median_variety` (Double): Giá trị trung vị của số lượng SKU khác nhau của các style thỏa điều kiện.

## 3. Task 2-1 (Spark Structured API)
- **Tên file:** `Task_2-1.parquet`
- **Định dạng:** Parquet (Single file)
- **Schema:**
  - `ship-city` (String): Tên thành phố.
  - `ship-state` (String): Tên bang (để tránh trùng lặp thành phố giữa các bang).
  - `cancelled_percentage` (Double): Tỷ lệ phần trăm đơn hàng bị hủy thỏa mãn các điều kiện phức hợp.

## 4. Task 2-2 (Spark Structured API)
- **Tên file:** `Task_2-2.parquet`
- **Định dạng:** Parquet (Single file)
- **Schema:**
  - `SKU` (String): Mã SKU.
  - `month` (String): Tháng.
  - `p90_stddev_approx` (Double): Độ lệch chuẩn Amount của nhóm P90 (tính bằng hàm có sẵn).
  - `p90_stddev_exact` (Double): Độ lệch chuẩn Amount của nhóm P90 (tính bằng cách tự cài đặt).
  - `p80_stddev_approx` (Double): Độ lệch chuẩn Amount của nhóm P80 (tính bằng hàm có sẵn).
  - `p80_stddev_exact` (Double): Độ lệch chuẩn Amount của nhóm P80 (tính bằng cách tự cài đặt).

---
**Lưu ý:** Tất cả các giá trị số thực (Double) nên làm tròn đến 4 chữ số thập phân để đảm bảo sự gọn gàng trong báo cáo.
