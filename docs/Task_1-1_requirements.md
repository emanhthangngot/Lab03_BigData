# Task 1-1: Sliding Window — Most Bought Size per State

**Phụ trách:** A — Lê Xuân Trí (23120099)  
**Framework:** MapReduce  
**Ngôn ngữ:** Scala  
**Output:** `Task_1-1.csv`

---

## Đề bài

Implement a sliding window computation that identifies the **size that is mostly bought** at each state within **maximum 7 days prior to the current date** (from at least d-7 to d-1, window length may be less than 7 if there is no appropriate past orders).

An item is considered **"bought"** if the associated order has a **"shipped" in its status** and the **quantity is non-zero**.

The window should **slide by 1 day** at a time, thus **unseen timestamp may arise** in the result which is expected for a sliding window's result.

---

## Giải thích chi tiết

### Sliding Window là gì?

Cho mỗi ngày `d` trong dataset, ta nhìn lại **tối đa 7 ngày trước** (từ `d-7` đến `d-1`, **KHÔNG** bao gồm ngày `d` hiện tại) để xác định size nào được mua nhiều nhất tại mỗi state.

**Ví dụ minh họa:**

Giả sử tại state MAHARASHTRA, ngày hiện tại là `04-10-22`:
- Window = [04-03-22, 04-09-22] (7 ngày trước)
- Trong window này, đếm số lần mua mỗi size (chỉ tính orders "shipped" + Qty > 0):
  - M: 50 lần → **nhiều nhất**
  - L: 40 lần
  - S: 30 lần
- Kết quả cho (MAHARASHTRA, 04-10-22) → Most bought size = **M**

Ngày hôm sau `04-11-22`, window trượt sang phải 1 ngày:
- Window = [04-04-22, 04-10-22]
- Lặp lại tính toán tương tự

### Điều kiện "bought"

Một item được coi là **"bought"** khi **đồng thời thỏa 2 điều kiện**:
- Cột `Status` chứa "shipped" (bao gồm cả "Shipped", "Shipped - Delivered to Buyer", etc.)
- Cột `Qty` > 0 (số lượng khác 0)

### Window length < 7

Nếu ngày `d` nằm gần đầu dataset (VD: ngày đầu tiên hoặc ngày thứ 3), thì window có thể **ngắn hơn 7 ngày** vì không có đủ dữ liệu quá khứ. Đây là hành vi bình thường.

**Ví dụ:** Nếu dataset bắt đầu từ `04-01-22`, thì:
- Ngày `04-02-22`: window = [04-01-22] (chỉ 1 ngày)
- Ngày `04-04-22`: window = [04-01-22, 04-03-22] (3 ngày)
- Ngày `04-08-22` trở đi: window đủ 7 ngày

### Unseen timestamps

Kết quả có thể chứa các **ngày không xuất hiện trong dataset gốc**. Ví dụ nếu không có order nào vào ngày `04-05-22`, nhưng khi slide window qua ngày đó, nó vẫn xuất hiện trong output. Đây là **expected behavior** của sliding window.

### Phạm vi kết quả

Kết quả là bảng **(State, Date, Most Bought Size)** — cho **mỗi state** tại **mỗi ngày** (mà window có dữ liệu), ghi nhận size nào được mua nhiều nhất trong 7 ngày trước đó.

---

## Quy định

- **Ngôn ngữ:** Scala (để được full điểm).
- Chỉ được dùng native code/libraries nếu Spark built-in APIs không đủ.
- Code phải **well-documented** với comments rõ ràng.

---

## Output

- Export kết quả ra **1 file CSV duy nhất** (`Task_1-1.csv`).
- File phải readable trên **normal filesystem** (KHÔNG phải Hadoop-specific files như part-00000).
- Nhóm tự chọn schema phù hợp.
- Upload lên **Google Drive** (link trong `drive_link.txt`).

---

## Report bắt buộc

1. Phân tích cách hiểu và frame query (correct analysis of the queries)
2. Phân tách query thành các bước cơ bản (successful decomposition into elemental steps)
3. Giải thích reasoning đằng sau cách phân tách đó
4. Mô tả chiến lược implementation cho từng bước
