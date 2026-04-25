# Tiêu Chí Chấm Điểm Chi Tiết — Lab 03

**Tổng điểm:** 10 điểm (4 tasks × 2.5đ)

---

## 1. Cơ cấu điểm cho mỗi bài tập (2.5đ)

Mỗi bài tập trong số 4 bài tập (1-1, 1-2, 2-1, 2-2) sẽ được chấm dựa trên các tiêu chí sau:

| STT | Yêu cầu thành phần | Điểm | Mô tả |
|:---:|---|:---:|---|
| 1 | Phân tích query đúng | 0.5 | Phân tích chính xác các yêu cầu của đề bài trong báo cáo. |
| 2 | Phân tách thành các bước cơ bản | 0.5 | Chia bài toán lớn thành các bước thực thi nhỏ (elemental steps). |
| 3 | Giải thích lý do phân tách | 0.5 | Lập luận logic tại sao lại chia các bước như vậy. |
| 4 | Cài đặt thành công | 0.5 | Source code chạy được và thực hiện đúng logic đã phân tích. |
| 5 | Kiểm thử và xuất dữ liệu | 0.2 | Xuất kết quả ra file thành công và đúng định dạng yêu cầu. |
| 6 | Độ chính xác của kết quả | 0.175 | Kết quả trong file output phải khớp với dữ liệu gốc. |
| 7 | Ngôn ngữ lập trình (Scala) | 0.125 | Sử dụng Scala cho giải pháp hoàn chỉnh. |
| | **Tổng cộng mỗi bài** | **2.5** | |

---

## 2. Quy định về Báo cáo (Report)

Báo cáo là thành phần quan trọng chiếm phần lớn số điểm (1.5đ / 2.5đ cho mỗi bài tập). Báo cáo cần chi tiết và bao gồm:
- Cách nhóm hiểu và đóng khung các truy vấn.
- Cách nhóm phân rã chúng.
- Chiến lược thực thi cho từng bước đã phân rã.

### Yêu cầu riêng cho Spark Structured APIs (Task 2-1 & 2-2)
- **Task 2-1:** Phải bao gồm output của `explain(true)` và phân tích về:
  - Chiến lược join vật lý (Physical join strategy).
  - Số lượng shuffle exchanges (Exchange nodes).
  - Số lượng stages được tạo ra.
- **Task 2-2:** So sánh 2 phương pháp tính percentile về:
  - Độ chính xác (sai lệch giữa approximate và exact).
  - Thời gian thực thi.
  - Phân tích các group có kết quả khác nhau.
  - Thảo luận về chiến lược phân mảnh (partitioning) nếu dữ liệu > 1,000 orders.

---

## 3. Quy định về Sản phẩm nộp bài

### Định dạng Output
- **Advanced MapReduce (1-1, 1-2):** Phải là file `.csv` đơn lẻ, có thể đọc được bằng hệ thống tệp thông thường.
- **Structured APIs (2-1, 2-2):** Phải là file `.parquet` đơn lẻ, có thể parse bằng Pandas hoặc Spark.

### Cấu trúc thư mục nộp bài
Thư mục gốc đặt tên theo MSSV người đại diện: `<RepresentativeID>`
- `src/`: Chứa các thư mục con `Task_1-1`, `Task_1-2`, `Task_2-1`, `Task_2-2`. Mỗi thư mục con chứa thư mục `source` và mã nguồn.
- `docs/`:
  - `Report.pdf`: Báo cáo hợp nhất.
  - `drive_link.txt`: Link Google Drive chứa các file output.
  - `README`: (Tùy chọn) Hướng dẫn chạy code.

---

## 4. Checklist kiểm tra cuối cùng

- [ ] Code có comment đầy đủ và rõ ràng.
- [ ] Báo cáo có đầy đủ các mục phân tích cho cả 4 bài tập.
- [ ] Kết quả benchmark (nếu có) được lấy trung bình từ ít nhất 5 lần chạy.
- [ ] Tên tệp nén là `<RepresentativeID>.zip`.
- [ ] Toàn bộ giải pháp là một khối thống nhất (không nộp rời rạc theo cá nhân).
