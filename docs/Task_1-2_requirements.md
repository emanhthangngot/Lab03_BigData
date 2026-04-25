# Task 1-2: State-level Median Variety per Month

**Phụ trách:** B — Nguyễn Lê Trung Trực (23120180)  
**Framework:** MapReduce  
**Ngôn ngữ:** Scala  
**Output:** `Task_1-2.csv`

---

## Đề bài

The **"variety"** of a style is defined as the **number of distinct SKU** associated with that style within a **specific time interval** and in a **specific geographical region**.

**Median variety** is used to estimate the variety of goods purchased within this time-space interval and it is computed as the **median value of all style** that satisfies a specific condition.

For each **month** (for example, July starts at 07-01 and ends at 07-31), you are required to calculate the **state-level median variety** of all style which has served a **size of at least XXL** (for example, XXL, 3XL, 4XL, etc.).

---

## Giải thích chi tiết

### "Variety" of a style nghĩa là gì?

Mỗi style (VD: `SET389`, `JNE3781`) có thể gắn với **nhiều SKU khác nhau** (VD: `SET389-KR-NP-S`, `SET389-KR-NP-M`, `SET389-KR-NP-L`). **Variety = số lượng SKU riêng biệt (distinct)** của style đó, tính trong 1 phạm vi cụ thể:
- **Time interval** = 1 tháng (VD: tháng 4 = 04-01 → 04-30)
- **Geographical region** = 1 state (VD: MAHARASHTRA)

**Ví dụ:** Style `SET389` tại MAHARASHTRA trong tháng 4/2022 xuất hiện với các SKU: `SET389-KR-NP-S`, `SET389-KR-NP-M`, `SET389-KR-NP-L` → variety = **3**.

### "Median variety" nghĩa là gì?

Sau khi tính variety cho **tất cả style** qualifying trong 1 (State, Month), lấy **giá trị trung vị (median)** của các variety đó.

**Ví dụ:** Tại MAHARASHTRA, tháng 4/2022, có 5 qualifying styles với variety lần lượt là [2, 3, 3, 5, 7]:
- Sort: [2, 3, **3**, 5, 7]
- Median = **3** (phần tử ở giữa)

Nếu có 4 styles: [2, 3, 5, 7] → Median = (3 + 5) / 2 = **4.0**

### Điều kiện "served a size of at least XXL"

Chỉ xét các style mà **đã có ít nhất 1 đơn hàng** với size **≥ XXL**. Các size qualifying gồm:
- `XXL`, `2XL`
- `XXXL`, `3XL`
- `4XL`, `5XL`, `6XL`, ...

**Lưu ý quan trọng:** Style chỉ cần có **ít nhất 1 record** có size ≥ XXL (trong cùng scope Month + State) là qualify. Khi tính variety thì vẫn **đếm tất cả distinct SKU** của style đó (kể cả các SKU size nhỏ hơn XXL).

### Phạm vi kết quả

Kết quả là bảng **(State, Month, Median Variety)** — cho mỗi state tại mỗi tháng, ghi nhận giá trị median variety của các qualifying styles.

---

## Quy định

- **Ngôn ngữ:** Scala (để được full điểm).
- Chỉ được dùng native code/libraries nếu Spark built-in APIs không đủ.
- Code phải **well-documented** với comments rõ ràng.

---

## Output

- Export kết quả ra **1 file CSV duy nhất** (`Task_1-2.csv`).
- File phải readable trên **normal filesystem** (KHÔNG phải Hadoop-specific files).
- Nhóm tự chọn schema phù hợp.
- Upload lên **Google Drive** (link trong `drive_link.txt`).

---

## Report bắt buộc

1. Phân tích cách hiểu và frame query (correct analysis of the queries)
2. Phân tách query thành các bước cơ bản (successful decomposition into elemental steps)
3. Giải thích reasoning đằng sau cách phân tách đó
4. Mô tả chiến lược implementation cho từng bước
