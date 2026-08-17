# Task 03 - Data contract và quarantine priority

## Mục tiêu

Chuẩn hóa `priority` về số nguyên `1..4`, giữ lại các biểu diễn hợp lệ, chuyển bản ghi hỏng vào quarantine và không làm dừng toàn bộ pipeline.

## Hiện tượng

Nguồn thay đổi `priority` từ số sang nhãn chữ. Cast trực tiếp làm nhãn hợp lệ thành `NULL`, trong khi các số ngoài miền như `0` hoặc `5` lại đi qua. Pipeline vẫn chạy nhưng chất lượng dữ liệu giảm.

## Yêu cầu bắt buộc

- `silver_tickets.priority` là integer, không NULL và thuộc `1..4`.
- `silver_tickets` vẫn có `12.480` ticket.
- `quarantine_tickets` có đúng `312` bản ghi CDC lỗi.
- Grain quarantine: `1 hàng / 1 bản ghi CDC`.
- Bật dbt contract cho `silver_tickets`.
- `dbt test` pass và có nhiều hơn 9 test.
- Không quarantine nhãn chữ hợp lệ.

## File cần chỉnh sửa

- `dbt/macros/normalize_priority.sql`
- `dbt/models/silver/silver_tickets.sql`
- `dbt/models/silver/quarantine_tickets.sql`
- `dbt/models/silver/schema.yml`

## Thứ tự chạy trên Windows PowerShell

### 1. Bật UTF-8 và cấu hình đường dẫn dbt

```powershell
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:LAB17_DB = Join-Path (Get-Location) "warehouse.duckdb"
$env:DBT_PROFILES_DIR = Join-Path (Get-Location) "dbt"
```

### 2. Tạo warehouse baseline

Nếu chưa có `warehouse.duckdb` hoặc muốn chạy lại từ đầu:

```powershell
Remove-Item warehouse.duckdb, warehouse.duckdb.wal -Force -ErrorAction SilentlyContinue
python tools/run_pipeline.py
```

Chạy kiểm tra nhanh:

```powershell
python tools/verify.py --runs 1
```

Baseline Task 03 phải thể hiện:

```text
dbt test:                                  9/9 pass
silver_tickets.priority sai:               6.606 hàng
quarantine_tickets:                        0 / 312
silver_tickets:                            12.480 ticket
```

### 3. Khảo sát phân bố priority

```powershell
@'
import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)

print("BRONZE PRIORITY")
for value, count in con.execute("""
    select priority_raw, count(*)
    from bronze_tickets_cdc
    group by 1
    order by 2 desc, 1
""").fetchall():
    print(repr(value), count)

print("\nSILVER PRIORITY")
for value, count in con.execute("""
    select priority, count(*)
    from silver_tickets
    group by 1
    order by 1 nulls last
""").fetchall():
    print(repr(value), count)

con.close()
'@ | python -
```

Phân bố Bronze đã đo:

| Giá trị | Số bản ghi | Phân loại |
|---|---:|---|
| `low` | 1.845 | Nhãn hợp lệ, quy đổi về 4 |
| `urgent` | 1.819 | Nhãn hợp lệ, quy đổi về 1 |
| `medium` | 1.783 | Nhãn hợp lệ, quy đổi về 3 |
| `4` | 1.748 | Số hợp lệ |
| `3` | 1.710 | Số hợp lệ |
| `1` | 1.705 | Số hợp lệ |
| `high` | 1.695 | Nhãn hợp lệ, quy đổi về 2 |
| `2` | 1.683 | Số hợp lệ |
| `0` | 49 | Lỗi |
| Chuỗi rỗng | 43 | Lỗi |
| `P1` | 39 | Lỗi |
| `unknown` | 39 | Lỗi |
| `P2` | 38 | Lỗi |
| `5` | 37 | Lỗi |
| NULL | 35 | Lỗi |
| `-1` | 32 | Lỗi |

Tổng nhóm lỗi là `312` bản ghi CDC. Không được quarantine bốn nhãn chữ hợp lệ.

### 4. Sửa macro chuẩn hóa

Mở `dbt/macros/normalize_priority.sql`:

1. Dùng một biểu thức `CASE` chung cho cả Silver và quarantine.
2. Giữ các chuỗi số `1..4` dưới kiểu integer.
3. Quy đổi `urgent=1`, `high=2`, `medium=3`, `low=4`.
4. Trả về NULL cho mọi giá trị còn lại.
5. Có thể bổ sung `priority_reject_reason` để phân loại nguyên nhân lỗi.

### 5. Sửa thứ tự xử lý CDC trong Silver

Mở `dbt/models/silver/silver_tickets.sql` và tổ chức CTE theo thứ tự:

```text
Chuẩn hóa priority
-> loại bản ghi có priority_clean là NULL
-> row_number theo ticket_id
-> lấy bản ghi mới nhất
-> loại op = 'd'
```

Không lọc lỗi sau `row_number()`, vì cách đó làm mất cả ticket khi update mới nhất bị
hỏng dù ticket còn trạng thái hợp lệ trước đó.

### 6. Sửa quarantine

Mở `dbt/models/silver/quarantine_tickets.sql`:

- Thay `where false` bằng điều kiện priority không chuẩn hóa được.
- Giữ grain một hàng trên mỗi bản ghi CDC.
- Không deduplicate theo `ticket_id`.
- Giữ `priority_raw`, `cdc_seq` và `reject_reason` để điều tra.

### 7. Bật contract và test miền giá trị

Mở `dbt/models/silver/schema.yml`:

1. Đổi `contract.enforced` sang `true`.
2. Thêm `not_null` cho `priority`.
3. Thêm `accepted_values` cho các số `1, 2, 3, 4`.
4. Đặt `quote: false` vì đây là integer.

### 8. Kiểm tra nhanh từ warehouse sạch

```powershell
Remove-Item warehouse.duckdb, warehouse.duckdb.wal -Force -ErrorAction SilentlyContinue
python tools/verify.py --runs 1
```

Kỳ vọng:

```text
silver_tickets.priority sai:  0
quarantine_tickets:           312 / 312
silver_tickets:               12.480 ticket
dbt test:                     nhiều hơn 9 test và tất cả pass
```

### 9. Chạy riêng dbt test

```powershell
Push-Location dbt
dbt test --profiles-dir . --target-path target --log-path logs
Pop-Location
```

Nếu dbt báo không tìm thấy warehouse, kiểm tra lại `$env:LAB17_DB` phải là đường dẫn
tuyệt đối được khai báo trước khi `Push-Location`.

### 10. Query xác minh cuối

```powershell
@'
import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)

print("silver rows:", con.execute(
    "select count(*) from silver_tickets"
).fetchone()[0])

print("invalid priority:", con.execute("""
    select count(*) from silver_tickets
    where priority is null or priority not between 1 and 4
""").fetchone()[0])

print("quarantine rows:", con.execute(
    "select count(*) from quarantine_tickets"
).fetchone()[0])

con.close()
'@ | python -
```

Kết quả mong đợi:

```text
silver rows: 12480
invalid priority: 0
quarantine rows: 312
```

### 11. Chạy verify đầy đủ

```powershell
python tools/verify.py
```

Task 03 hoàn thành khi contract được bật, toàn bộ dbt test pass, Silver vẫn đủ 12.480
ticket, priority chỉ thuộc `1..4`, quarantine đúng 312 hàng và checksum ổn định qua ba
lượt chạy.

## Các bước thực hiện

### Bước 1 - Khảo sát dữ liệu thô và Silver

```sql
select priority_raw, count(*)
from bronze_tickets_cdc
group by 1
order by 2 desc;
```

```sql
select priority, count(*)
from silver_tickets
group by 1
order by 1;
```

Phân loại ba nhóm:

1. Chuỗi số hợp lệ: `1`, `2`, `3`, `4`.
2. Nhãn hợp lệ: `urgent`, `high`, `medium`, `low`.
3. Giá trị hỏng: NULL, rỗng, chuỗi lạ hoặc số ngoài miền.

### Bước 2 - Chuẩn hóa tại một macro dùng chung

Thay cast trực tiếp bằng biểu thức `CASE` trong `normalize_priority`:

- Nhóm số hợp lệ được giữ nguyên dưới kiểu integer.
- Nhóm nhãn hợp lệ được ánh xạ đúng theo contract API.
- Mọi giá trị còn lại trả về NULL.

Không viết hai logic khác nhau cho Silver và quarantine. Hai model phải dùng cùng macro để tránh lệch định nghĩa hợp lệ.

### Bước 3 - Quarantine bản ghi lỗi

Thay điều kiện `where false` bằng điều kiện nhận diện giá trị không chuẩn hóa được.

Quarantine phải giữ thông tin phục vụ điều tra:

- `ticket_id`
- `cdc_seq`
- `op`
- `event_time`
- `_ingested_at`
- `priority_raw`
- `reject_reason`

Không deduplicate quarantine theo ticket vì grain yêu cầu một hàng trên mỗi bản ghi CDC lỗi.

### Bước 4 - Lọc trước khi chọn trạng thái mới nhất

Trong `silver_tickets`, loại bản ghi CDC có priority không hợp lệ trước khi chạy `row_number()`.

Thứ tự đúng về mặt nghiệp vụ:

```text
Chuẩn hóa -> loại bản ghi lỗi -> xếp hạng CDC -> lấy bản ghi mới nhất -> loại delete
```

Nếu xếp hạng trước rồi mới loại lỗi, ticket có update mới nhất bị hỏng sẽ biến mất hoàn toàn dù nó còn một trạng thái hợp lệ trước đó.

### Bước 5 - Bật contract

Trong `schema.yml`:

- Bật `contract.enforced` cho `silver_tickets`.
- Giữ kiểu `priority` là integer.

Contract kiểm tra kiểu dữ liệu nhưng không kiểm tra miền giá trị.

### Bước 6 - Thêm test miền giá trị

Bổ sung cho `priority`:

- `not_null`
- `accepted_values` với các số hợp lệ
- `quote: false`

Giữ các test unique và not-null của `ticket_id`.

### Bước 7 - Kiểm tra riêng dbt

```bash
make quick
make dbt-test
```

Query xác minh:

```sql
select count(*) from quarantine_tickets;

select count(*)
from silver_tickets
where priority is null or priority not between 1 and 4;

select count(*) as rows,
       count(distinct ticket_id) as tickets
from silver_tickets;
```

### Bước 8 - Kiểm tra hồi quy toàn pipeline

```bash
make verify
```

Xác nhận cả Task 01 và Task 02 vẫn đạt.

## Tiêu chí hoàn thành

- Contract được bật.
- `dbt test` pass với số test lớn hơn baseline 9.
- Quarantine đúng `312` hàng.
- Silver đúng `12.480` ticket.
- Không có priority NULL hoặc ngoài miền.
- Pipeline tiếp tục xử lý dữ liệu tốt khi gặp bản ghi lỗi.

## Nội dung cần ghi vào báo cáo

- Ba nhóm priority và cách xử lý từng nhóm.
- Vì sao cast trực tiếp sai theo hai hướng.
- Vì sao phải lọc trước khi xếp hạng CDC.
- Vì sao quarantine phù hợp hơn làm dừng toàn bộ DAG.
- Số hàng quarantine, Silver và kết quả test.
