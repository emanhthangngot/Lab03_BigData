# Hướng dẫn Cài đặt & Chạy Dự án Lab 03 (Scala + Spark)

Dự án này đã được cấu hình sẵn sử dụng **SBT (Scala Build Tool)**. Nhờ đó, việc quản lý các package (thư viện Spark, Hadoop) được tự động hoá hoàn toàn. Bạn không cần tải thủ công bất kỳ file `.jar` nào.

Dưới đây là các bước để setup môi trường code trên máy tính của bạn:

## 1. Yêu cầu hệ thống cơ bản (Prerequisites)

Trước khi mở code, hãy đảm bảo máy bạn đã cài các phần mềm sau:

1. **Java Development Kit (JDK):** Yêu cầu cài đặt JDK 11 trên Linux/WSL.
2. **SBT (Scala Build Tool):** Phần mềm quản lý thư viện và build project cho Scala (cài đặt qua Terminal).
3. **IDE (Phần mềm viết code):**
   - **Khuyến nghị số 1:** **IntelliJ IDEA** (bản Community miễn phí).
   - **Lựa chọn 2:** **Visual Studio Code** (yêu cầu cài thêm extension).

## 2. Hướng dẫn Mở và Tải thư viện bằng IDE

### Lựa chọn A: Dùng IntelliJ IDEA (Khuyến nghị)
1. Cài đặt **IntelliJ IDEA**.
2. Mở IntelliJ, vào mục **Plugins**, tìm và cài đặt plugin **Scala** của JetBrains. Sau đó khởi động lại IDE.
3. Mở IntelliJ, chọn **Open** -> Chọn thư mục `Lab03` (chứa file `build.sbt`).
4. IntelliJ sẽ nhận diện đây là một project SBT. Khi thấy thanh thông báo xuất hiện ở góc dưới bên phải hoặc trên cùng, hãy bấm **Load SBT Project**.
5. Đợi IDE tự động tải tất cả các thư viện Spark và Hadoop về máy. Quá trình này diễn ra ngầm (khoảng 2-5 phút). Bạn sẽ thấy thư mục code hết bị báo lỗi đỏ.

### Lựa chọn B: Dùng VS Code
1. Cài đặt extension **Scala (Metals)** trong VS Code.
2. Mở thư mục `Lab03` bằng VS Code.
3. Extension Metals sẽ tự nhận diện file `build.sbt` và hiện một pop-up hỏi *"Import build?"*.
4. Nhấn **Import build** và chờ Metals tải thư viện về.

---

## 3. Hướng dẫn Setup trên Linux / WSL (Theo chuẩn Lab 1)

Vì Lab 1 yêu cầu chạy trên môi trường Linux (hoặc WSL trên Windows), bạn sẽ thiết lập trực tiếp qua Terminal. Spark sẽ chạy tính toán cục bộ (`local mode`) tự nhiên trên môi trường này mà không bị lỗi.

**Cách cài đặt các công cụ cơ bản qua Terminal (Ubuntu/Debian):**

1. **Cài đặt Java (JDK 11):**
   ```bash
   sudo apt update
   sudo apt install openjdk-11-jdk -y
   ```
   *Kiểm tra lại xem Java đã nhận chưa bằng lệnh `java -version`.*

2. **Cài đặt SBT:**
   ```bash
   sudo apt-get update
   sudo apt-get install apt-transport-https curl gnupg -yqq
   echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
   echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
   curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
   sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
   sudo apt-get update
   sudo apt-get install sbt -y
   ```

3. **Làm việc với IDE trên môi trường WSL (Nếu dùng Windows):**
   - **Với VS Code:** Cài extension **WSL**. Mở VS Code, click vào biểu tượng màu xanh ở góc dưới cùng bên trái (><) và chọn **"Connect to WSL"**. Sau đó điều hướng mở thư mục `Lab03` và để Metals tải thư viện như phần 2.
   - **Với IntelliJ IDEA:** Cứ mở IntelliJ như bình thường. Khi chọn **Open**, bạn hãy đi tới thư mục mạng của WSL (thường là `\\wsl$\Ubuntu\home\tên-user\...` hoặc `\\wsl.localhost\Ubuntu\...`) để mở thư mục `Lab03`. IDE sẽ nhận diện hoàn hảo như một project Linux.

---

## 4. Cách Chạy Code

Project đã được chia làm 4 thư mục ứng với 4 Task. Mỗi Task có 1 file `.scala` riêng biệt có chứa hàm `main`.

- **Với IntelliJ IDEA:** Bạn chỉ cần mở file `Task1_1.scala` (nằm trong `src/Task_1-1/src/main/scala/`), bấm vào nút **Play (mũi tên màu xanh)** bên cạnh chữ `object Task1_1` để chạy.
- **Dữ liệu đầu vào:** Hãy đặt file `Amazon Sale Report.csv` trực tiếp trong thư mục `Lab03` (ngang hàng với file `build.sbt` này). 
- **Cách đọc data:** Trong code, dùng đường dẫn tương đối:
  ```scala
  val df = spark.read.option("header", "true").csv("Amazon Sale Report.csv")
  ```

Chúc các bạn code vui vẻ và đạt full điểm môn Big Data!
