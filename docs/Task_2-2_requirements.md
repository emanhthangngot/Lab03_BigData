# Task 2-2: StdDev of Amount by SKU-Month with Percentile Threshold

**Phụ trách:** D — Nguyễn Hồ Anh Tuấn (23120185)  
**Framework:** Spark Structured APIs (DataFrame/Dataset API)  
**Ngôn ngữ:** Scala  
**Output:** `Task_2-2.parquet`

---

## Đề bài

For each **SKU** within each **month**, compute the **standard deviation** of the amount of orders whose **number of promotions meets a dynamic percentile threshold**. Specifically, two percentile levels are required:

**P90 (90th percentile):** Select all orders whose number of promotions is **at or above the 90th percentile** of promotion counts within that SKU-month group. Compute the **population standard deviation (degree of freedom = 0)** of the purchased amounts of these selected orders.

**P80 (80th percentile):** Apply the same logic using the **80th percentile** as the threshold.

The number of promotions for each order is determined by counting **all promotion identifiers** associated with that order (including Amazon-issued promotions). If a SKU-month group contains **fewer than 2 qualifying orders** after the percentile filter, the standard deviation is **set to zero**.

Must implement **two approaches** for computing the percentile thresholds:
1. Using Spark's built-in `approx_percentile` (or `percentile_approx`) function.
2. A self-implemented **exact percentile** computation using DataFrame/Dataset operations.

---

## Giải thích chi tiết

### Bước logic tổng quan

Bài này yêu cầu: trong mỗi nhóm **(SKU, Month)**, chỉ giữ lại các orders có **số lượng promotions "cao"** (theo percentile), rồi tính **độ lệch chuẩn** trên Amount của các orders còn lại.

### Đếm promotions per order

Mỗi order có cột `promotion-ids` (comma-separated). Đếm tất cả promotion identifiers trong đó = **promotion count** của order.

**Ví dụ:** Order A có `promotion-ids` = `"AAT-ABC, AAT-DEF, AAT-GHI"` → promotion count = **3**.  
Order B có `promotion-ids` = null/empty → promotion count = **0**.

### Percentile threshold hoạt động thế nào?

Trong mỗi nhóm (SKU, Month), ta có danh sách promotion counts của tất cả orders. Tính percentile trên danh sách đó:

**Ví dụ:** Nhóm (SKU = "JNE3405-KR-XL", Month = April) có 10 orders với promotion counts:
```
[0, 0, 1, 2, 3, 5, 8, 10, 15, 20]
```
- P90 threshold ≈ 18 → chỉ giữ orders có count ≥ 18 → giữ order có count = 20
- P80 threshold ≈ 14 → giữ orders có count ≥ 14 → giữ orders có count = 15, 20

### Population stddev (ddof=0)

Dùng **population standard deviation** (chia cho N, không phải N-1):

```
σ = sqrt( Σ(xi - μ)² / N )
```

Nếu sau khi lọc chỉ còn **< 2 orders** → stddev = **0** (không đủ data để tính).

### Hai approaches bắt buộc implement

**Approach 1 — Built-in approximate:**
- Dùng `approx_percentile` / `percentile_approx` của Spark
- Đây là hàm **xấp xỉ**, có thể cho kết quả hơi khác so với exact

**Approach 2 — Self-implemented exact:**
- Tự code logic tính percentile chính xác bằng DataFrame/Dataset operations (VD: sort + row_number + tính vị trí percentile)
- Kết quả phải **chính xác** (exact)

Cả 2 approaches cần chạy và **so sánh kết quả** trong report.

---

## Quy định

- Solution phải dùng **DataFrame/Dataset API**.
- **Ngôn ngữ:** Scala (để được full điểm).
- Chỉ được dùng native code/libraries nếu Spark built-in APIs không đủ.
- Code phải **well-documented** với comments rõ ràng.
- Benchmark phải chạy **ít nhất 5 lần**, report **mean ± standard deviation**.

---

## Output

- Export kết quả ra **1 file PARQUET duy nhất** (`Task_2-2.parquet`).
- File phải parseable bởi **Pandas hoặc Spark local mode** trên **normal filesystem**.
- Nhóm tự chọn schema phù hợp.
- Upload lên **Google Drive** (link trong `drive_link.txt`).

---

## Report bắt buộc

1. Phân tích cách hiểu và frame query
2. Phân tách query thành các bước cơ bản
3. Giải thích reasoning đằng sau cách phân tách đó
4. Mô tả chiến lược implementation cho từng bước
5. **BẮT BUỘC — So sánh 2 approaches:**
   - **(a) Accuracy:** Sự khác biệt giữa approximate và exact thresholds
   - **(b) Execution time:** Benchmark ≥ 5 lần, report mean ± stddev
   - **(c) Analysis:** Các SKU-month groups mà 2 approaches cho qualifying orders khác nhau
6. **NẾU CÓ** SKU-month group chứa > 1,000 orders, phải thảo luận:
   - **(a)** Manual repartitioning có beneficial không?
   - **(b)** Lý do đằng sau partition strategy đã chọn
   - **(c)** Spark default partition size (128 MB) relate thế nào với data volume của group
