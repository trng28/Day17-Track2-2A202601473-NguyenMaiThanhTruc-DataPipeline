# Task 04 - Tối ưu dashboard Parquet

> Bài mở rộng, tối đa cộng 5 điểm.

## Mục tiêu

Giảm tối thiểu 10 lần số hàng phải scan cho dashboard mà không thay đổi kết quả query.

## Hiện tượng

Dataset gồm khoảng 5.000 file Parquet nhỏ, không partition và hàng có thứ tự ngẫu nhiên. Query lọc một khách hàng trong một ngày nhưng engine vẫn phải mở gần như toàn bộ file.

## Yêu cầu bắt buộc

- `rows scanned` giảm ít nhất 10 lần.
- Số file giảm từ khoảng 5.000 xuống hàng chục.
- `result hash` giữ nguyên.
- Không sửa `tools/explain.py` hoặc baseline để đạt tiêu chí.

## File cần chỉnh sửa

- `tools/compact.py`
- `queries/dashboard.sql`

## Thứ tự chạy trên Windows PowerShell

### 1. Bật UTF-8

```powershell
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
```

### 2. Sinh dataset bài mở rộng

```powershell
python seed/generate.py --extra
```

Kiểm tra số file:

```powershell
(Get-ChildItem data\gold_events\*.parquet).Count
```

Kết quả phải là `5000`.

### 3. Đo baseline

```powershell
python tools/explain.py
python tools/explain.py --plan
```

Baseline đã đo:

| Chỉ số | Giá trị trước tối ưu |
|---|---:|
| Rows scanned | 5.000.000 |
| Rows on disk | 130.683 |
| Files | 5.000 |
| Result hash | `4379e4c5d9f3` |
| Mức giảm | 1,0 lần |
| Mục tiêu rows scanned | Không quá 500.000 |

Kết quả nghiệp vụ của query baseline:

```text
('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)
```

### 4. Đo cardinality và kích thước partition tiềm năng

```powershell
@'
import duckdb

con = duckdb.connect()
source = "read_parquet('data/gold_events/*.parquet')"

print("distinct event_date:", con.execute(
    f"select count(distinct event_date) from {source}"
).fetchone()[0])

print("distinct customer_name:", con.execute(
    f"select count(distinct customer_name) from {source}"
).fetchone()[0])

print("rows/day min avg max:", con.execute(f"""
    select min(n), round(avg(n), 2), max(n)
    from (
        select event_date, count(*) as n
        from {source}
        group by 1
    )
""").fetchone())

con.close()
'@ | python -
```

Số liệu đã đo:

```text
event_date distinct:     14
customer_name distinct:  650
rows/day min:            9.233
rows/day average:        9.334,5
rows/day max:            9.421
target day rows:         9.382
ACME target day rows:    3.500
```

### 5. Chọn storage layout

Dựa trên số liệu trên:

- Partition theo `event_date`, tạo 14 partition và cho phép bỏ qua 13 ngày không liên quan.
- Không partition theo `customer_name`, vì 650 giá trị sẽ tạo quá nhiều thư mục/file nhỏ.
- Trong mỗi partition ngày, sort theo `customer_name` rồi `event_time` để các hàng cùng khách hàng nằm gần nhau.
- Chọn row group nhỏ hơn số hàng trung bình mỗi ngày. Một lựa chọn cần thử nghiệm là `2048`, giúp một partition khoảng 9.300 hàng có nhiều row group và cho phép min/max statistics loại các nhóm khách hàng không liên quan.

Không chọn row group lớn hơn toàn bộ partition ngày, vì khi đó min/max của một row group
có thể bao phủ tất cả khách hàng trong ngày và không hỗ trợ filter `customer_name`.

### 6. Implement `tools/compact.py`

Hoàn thiện khung `COPY ... TO` với các quyết định đã đo:

```text
Nguồn:       data/gold_events/*.parquet
Đích:        data/gold_events_v2
Partition:   event_date
Sort:        customer_name, event_time
Row group:   bắt đầu thử nghiệm với 2048
```

Sau khi ghi, bắt buộc assert số hàng nguồn và đích đều là `130.683`.

### 7. Cập nhật query dashboard

Trong `queries/dashboard.sql`:

1. Đọc toàn bộ file Parquet bên dưới `data/gold_events_v2/` theo recursive glob.
2. Bật `hive_partitioning`.
3. Lọc trực tiếp trên `event_date` thay vì bọc `event_time` bằng `strftime`.
4. Giữ nguyên filter `customer_name` và toàn bộ phép tổng hợp.

Predicate ngày phải có dạng cột đứng độc lập ở một vế để engine dùng partition pruning.

### 8. Chạy compact và đo lại

```powershell
python tools/compact.py
python tools/explain.py
python tools/explain.py --plan
```

Đối chiếu:

```text
rows scanned <= 500.000
files hiện tại < 5.000
result hash = 4379e4c5d9f3
```

Nếu result hash thay đổi, thay đổi query đã làm sai ngữ nghĩa và phải được sửa trước khi
đánh giá hiệu năng.

### 9. Kiểm tra hồi quy

```powershell
python tools/verify.py
```

Task 04 đạt khi dashboard giảm tối thiểu 10 lần rows scanned, số file giảm, result hash
không đổi và các task bắt buộc không bị ảnh hưởng.

## Các bước thực hiện

### Bước 1 - Sinh dữ liệu mở rộng

```bash
make seed-extra
```

Lệnh tạo `data/gold_events/` và baseline.

### Bước 2 - Đo baseline

```bash
make explain
make plan
```

Ghi lại:

- Rows scanned
- Rows on disk
- Số file
- Result hash
- Cây `EXPLAIN ANALYZE`

### Bước 3 - Phân tích predicate

Đọc `queries/dashboard.sql`:

- Query lọc theo ngày và khách hàng.
- Predicate ngày đang bọc cột trong function nên không sargable.
- Dataset hiện tại không đưa thông tin filter vào path.

Viết lại filter ngày theo khoảng timestamp hoặc cột partition để engine có thể prune trước khi đọc dữ liệu.

### Bước 4 - Chọn partition hợp lý

Chọn cột có cardinality vừa phải và xuất hiện trong filter phổ biến. Không partition trực tiếp theo cột có hàng trăm giá trị nếu việc đó tạo lại small-file problem.

Ghi rõ:

- Số giá trị phân biệt của cột partition.
- Số thư mục dự kiến.
- Lý do engine có thể bỏ qua partition không liên quan.

### Bước 5 - Chọn sort order và row group

Sắp các hàng để dữ liệu cùng filter phụ nằm gần nhau. Chọn `ROW_GROUP_SIZE` tương thích với số hàng mỗi partition để min/max statistics có ích.

Ba quyết định phải giải thích được:

1. Partition column.
2. Sort columns.
3. Row group size.

### Bước 6 - Implement compact

Trong `tools/compact.py`:

- Đọc toàn bộ dataset cũ.
- Ghi dataset mới bằng `COPY ... TO`.
- Bật partitioning.
- Sắp thứ tự hàng.
- Chọn row group size.
- Đảm bảo chạy lại không để dữ liệu cũ gây trùng.
- Assert số hàng nguồn và đích bằng nhau.

### Bước 7 - Cập nhật query

Trong `queries/dashboard.sql`:

- Trỏ sang dataset mới.
- Bật Hive partitioning khi đọc.
- Dùng predicate sargable.
- Giữ nguyên các phép tổng hợp và ngữ nghĩa kết quả.

### Bước 8 - Đo sau tối ưu

```bash
make compact
make explain
make plan
```

So sánh trước/sau. Nếu hash đổi, dừng và sửa ngữ nghĩa query trước khi tối ưu tiếp.

### Bước 9 - Kiểm tra hồi quy

```bash
make verify
```

## Tiêu chí hoàn thành

- Rows scanned giảm ít nhất 10 lần.
- Result hash không đổi.
- Số file giảm rõ rệt.
- Số hàng nguồn bằng số hàng dataset mới.
- Ba task bắt buộc vẫn đạt.

## Nội dung cần ghi vào báo cáo

- Layout cũ và nguyên nhân small-file problem.
- Rows scanned, files, rows on disk trước/sau.
- Partition, sort order và row group size đã chọn.
- Result hash trước/sau.
