# Task 2-1: Percentage of Cancelled Orders per City

**Phụ trách:** C — Trần Hữu Kim Thành (23120166)  
**Framework:** Spark Structured APIs (DataFrame/Dataset API)  
**Ngôn ngữ:** Scala  
**Output:** `Task_2-1.parquet`

---

## Đề bài

For each **city**, calculate the **percentage of cancelled orders** of **Standard service level** that possess **at least 3 temporally-valid promotions** while having the **purchased amount less than the average amount** of the associated state's **merchant-fulfillment orders** which have a **courier status of "Shipped"**.

### Temporally-valid promotion

A promotion is considered **temporally valid** if its **active period spans at least 2 days**. The active period of a promotion is derived from the dataset as follows: for each **unique promotion identifier** appearing across **all orders**, its **first appearance date** and **last appearance date** are computed; the active period is defined as the **number of days between these two dates**.

All promotions including those issued by Amazon, are counted towards the criterion of 3 simultaneous ones, provided that they satisfy the temporal validity condition above.

---

## Giải thích chi tiết

### Tổng quan query

Bài này yêu cầu tính **phần trăm đơn bị cancelled** trong số các đơn thỏa **đồng thời 3 điều kiện**, nhóm theo từng **city**.

### 3 điều kiện đồng thời

**Điều kiện 1 — Service level:**
- Cột `ship-service-level` = "Standard"

**Điều kiện 2 — Ít nhất 3 temporally-valid promotions:**
- Mỗi order có cột `promotion-ids` chứa danh sách promotion IDs (phân tách bằng dấu phẩy)
- Cho mỗi **unique promotion ID trên toàn bộ dataset**: tìm ngày xuất hiện đầu tiên (min Date) và ngày xuất hiện cuối cùng (max Date)
- **Active period** = max Date − min Date (tính bằng số ngày)
- Promotion **valid** nếu active period **≥ 2 ngày**
- Order phải có **≥ 3 promotions valid** (kể cả promotions do Amazon phát hành)

**Ví dụ:** Promotion ID `AAT-WNKTBO3K27EJC` xuất hiện lần đầu ngày 04-01-22 và lần cuối ngày 04-15-22 → active period = 14 ngày ≥ 2 → **valid**. Nếu promotion chỉ xuất hiện đúng 1 ngày → active period = 0 → **invalid**.

**Điều kiện 3 — Amount < average amount của Merchant-Shipped cùng state:**
- Tính **average Amount** của tất cả orders trong cùng state mà:
  - `Fulfilment` = "Merchant"
  - `Courier Status` = "Shipped"
- Order hiện tại phải có `Amount` **nhỏ hơn** giá trị average này

**Ví dụ:** Tại state MAHARASHTRA, average amount của Merchant + Shipped orders = 500 INR. Một order có Amount = 400 → thỏa điều kiện (400 < 500).

### Tính percentage

Sau khi lọc ra tất cả orders thỏa đồng thời 3 điều kiện trên, nhóm theo **city** (`ship-city`):

```
percentage = (số orders cancelled / tổng số orders thỏa ĐK) × 100
```

Trong đó "cancelled" dựa vào cột `Status`.

---

## Quy định

- Solution **must exclusively use the DataFrame/Dataset API**. Direct **Spark SQL string queries are NOT accepted**.
  - Spark SQL có thể dùng để minh họa understanding hoặc làm intermediate steps, nhưng **KHÔNG được chấm điểm**.
- **Ngôn ngữ:** Scala (để được full điểm).
- Chỉ được dùng native code/libraries nếu Spark built-in APIs không đủ.
- Code phải **well-documented** với comments rõ ràng.

---

## Output

- Export kết quả ra **1 file PARQUET duy nhất** (`Task_2-1.parquet`).
- File phải parseable bởi **Pandas hoặc Spark local mode** trên **normal filesystem**.
- Nhóm tự chọn schema phù hợp.
- Upload lên **Google Drive** (link trong `drive_link.txt`).

---

## Report bắt buộc

1. Phân tích cách hiểu và frame query (correct analysis of the queries)
2. Phân tách query thành các bước cơ bản (successful decomposition into elemental steps)
3. Giải thích reasoning đằng sau cách phân tách đó
4. Mô tả chiến lược implementation cho từng bước
5. **BẮT BUỘC:** Include output của `explain(true)` (extended execution plan)
6. **BẮT BUỘC:** Phân tích **physical join strategy** Spark chọn (VD: BroadcastHashJoin, SortMergeJoin, BroadcastNestedLoopJoin)
7. **BẮT BUỘC:** Đếm số **shuffle exchanges** (Exchange nodes trong plan)
8. **BẮT BUỘC:** Đếm số **stages** produced by the query
