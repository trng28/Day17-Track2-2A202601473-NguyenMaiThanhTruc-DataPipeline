# Task 02 - Xử lý event đến muộn

## Mục tiêu

Đưa các event đến kho muộn vào đúng partition lịch sử của `gold_feature_daily`, đồng thời giữ model idempotent khi một cửa sổ ngày được tính lại.

## Hiện tượng

`gold_feature_daily` ổn định qua nhiều lần chạy nhưng thiếu dữ liệu ở các ngày quá khứ. Bảng hiện có khoảng `8.645` hàng thay vì `9.100`.

## Yêu cầu bắt buộc

- Số hàng cuối cùng: `9.100`.
- Grain: `1 hàng / (event_date, customer_id)`.
- Lookback phải dựa trên P99 độ trễ đo từ dữ liệu.
- Các partition trong lookback được thay thế, không append trùng.
- Kết quả vẫn ổn định qua ba lần chạy.

## File cần chỉnh sửa

- `dbt/models/gold/gold_feature_daily.sql`

## Thứ tự chạy trên Windows PowerShell

### 1. Bật UTF-8 cho Python và dbt

Chạy lại hai biến này khi mở terminal PowerShell mới:

```powershell
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
```

### 2. Sinh dữ liệu seed

Nếu chưa có các file JSONL trong `seed/`:

```powershell
python seed/generate.py
```

Repo đang có `expected/dashboard_baseline.json`, vì vậy nên sinh thêm dataset của bài mở
rộng để `verify.py` không dừng do thiếu file Parquet:

```powershell
python seed/generate.py --extra
```

Kiểm tra:

```powershell
(Get-ChildItem data\gold_events\*.parquet).Count
```

Kết quả mong đợi là `5000`.

### 3. Xóa warehouse cũ

```powershell
Remove-Item warehouse.duckdb, warehouse.duckdb.wal -Force -ErrorAction SilentlyContinue
```

### 4. Chạy pipeline baseline

```powershell
python tools/run_pipeline.py
```

### 5. Chạy kiểm tra nhanh trước khi sửa

`make quick` tương đương lệnh PowerShell sau:

```powershell
python tools/verify.py --runs 1
```

Baseline của Task 02 phải cho thấy:

```text
gold_feature_daily: 8.645 / 9.100
```

### 6. Đo P99 độ trễ

```powershell
@'
import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)
result = con.execute("""
with delays as (
    select date_diff('second', event_time, _ingested_at) as delay_seconds
    from bronze_events
)
select
    quantile_cont(delay_seconds, 0.50) as p50,
    quantile_cont(delay_seconds, 0.95) as p95,
    quantile_cont(delay_seconds, 0.99) as p99,
    max(delay_seconds) as max_delay,
    round(
        100 * avg(case when delay_seconds > 86400 then 1.0 else 0.0 end),
        4
    ) as late_over_1d_pct
from delays
""").fetchone()

print("P50:", result[0], "seconds")
print("P95:", result[1], "seconds")
print("P99:", result[2], "seconds")
print("P99 days:", result[2] / 86400)
print("Max:", result[3], "seconds")
print("Late > 1 day:", result[4], "%")
con.close()
'@ | python -
```

Số liệu đã đo từ seed hiện tại:

```text
P50: 11.067 giây
P95: 156.703,05 giây
P99: 235.512 giây = 2,7258 ngày
Max: 254.421 giây = 2,9447 ngày
Event trễ hơn 1 ngày: 5,0509%
```

Vì P99 là `2,7258 ngày`, lookback được chọn là `3 ngày` sau khi làm tròn lên.

### 7. Sửa model Task 02

Mở `dbt/models/gold/gold_feature_daily.sql`, sau đó:

1. Khai báo composite key `(event_date, customer_id)`.
2. Chọn incremental strategy thay thế hàng cũ khi cửa sổ được tính lại.
3. Đổi filter incremental thành lookback 3 ngày.
4. Giữ nguyên grain và các phép tổng hợp hiện có.

### 8. Xóa warehouse và kiểm tra lại từ trạng thái sạch

```powershell
Remove-Item warehouse.duckdb, warehouse.duckdb.wal -Force -ErrorAction SilentlyContinue
python tools/verify.py --runs 1
```

Kỳ vọng sau sửa:

```text
gold_feature_daily: 9.100 / 9.100
```

### 9. Kiểm tra trùng grain

```powershell
@'
import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)
con.sql("""
select event_date, customer_id, count(*) as n
from gold_feature_daily
group by 1, 2
having count(*) > 1
""").show()
con.close()
'@ | python -
```

Kết quả phải là bảng rỗng.

### 10. Chạy kiểm tra đầy đủ

```powershell
python tools/verify.py
```

Task 02 hoàn thành khi bảng có đúng `9.100` hàng, không trùng grain và checksum giống
nhau qua ba lượt chạy.

## Các bước thực hiện

### Bước 1 - Tái hiện thiếu dữ liệu

```bash
make quick
```

Ghi số hàng `gold_feature_daily` và đối chiếu với `expected/gold_feature_daily.count`.

### Bước 2 - Phân biệt event time và ingestion time

- `event_time`: lúc sự kiện xảy ra.
- `_ingested_at`: lúc sự kiện tới kho.
- `event_date`: ngày nghiệp vụ dùng để tổng hợp.

Một event xảy ra ngày cũ nhưng được ingest hôm nay phải cập nhật lại ngày nghiệp vụ cũ.

### Bước 3 - Đo phân bố độ trễ

Chạy query trên `bronze_events` để đo `_ingested_at - event_time`. Báo cáo tối thiểu:

- P50
- P95
- P99
- Max
- Tỷ lệ event trễ hơn 1 ngày

Ví dụ cấu trúc query:

```sql
with delays as (
    select date_diff('second', event_time, _ingested_at) as delay_seconds
    from bronze_events
)
select
    quantile_cont(delay_seconds, 0.50) as p50_seconds,
    quantile_cont(delay_seconds, 0.95) as p95_seconds,
    quantile_cont(delay_seconds, 0.99) as p99_seconds,
    max(delay_seconds) as max_seconds,
    avg(case when delay_seconds > 86400 then 1.0 else 0.0 end) as late_over_1d_ratio
from delays;
```

Quy đổi P99 sang số ngày theo hướng làm tròn lên để chọn lookback.

### Bước 4 - Phân tích filter hiện tại

Model hiện chỉ lấy ngày lớn hơn `max(event_date)` của bảng đích. Mô phỏng:

```text
event_date    = 2026-08-12
_ingested_at  = 2026-08-15
max ở đích    = 2026-08-14 hoặc 2026-08-15
```

Giải thích vì sao event trên không thỏa điều kiện chỉ lấy ngày mới hơn max.

### Bước 5 - Thiết kế lookback

Điều chỉnh incremental filter để mỗi lần chạy tính lại một khoảng ngày lùi từ mốc mới nhất trong bảng đích.

Lookback cần:

- Bao phủ P99 độ trễ đã đo.
- Không dùng một số tùy ý mà không có bằng chứng.
- Cân bằng độ đầy đủ dữ liệu và chi phí tính lại.

Trong báo cáo, giải thích vì sao P99 thường phù hợp hơn max và nêu rủi ro của 1% ngoại lệ còn lại.

### Bước 6 - Thiết kế khóa tổng hợp

Vì grain có hai cột, cấu hình `unique_key` phải đại diện cho cặp:

```text
(event_date, customer_id)
```

Chọn incremental strategy để các hàng trong lookback được thay thế bằng kết quả tổng hợp mới, không cộng dồn.

### Bước 7 - Kiểm tra nhanh

```bash
make quick
```

Kiểm tra khóa trùng:

```sql
select event_date, customer_id, count(*) as n
from gold_feature_daily
group by 1, 2
having count(*) > 1;
```

Query phải trả về 0 hàng.

### Bước 8 - Kiểm tra hồi quy

```bash
make verify
```

Xác nhận:

- `gold_feature_daily = 9.100`.
- Checksum ba lần giống nhau.
- `gold_training_set` vẫn đạt.
- `gold_doc_chunks` không thay đổi.

## Tiêu chí hoàn thành

- Đo và ghi được P99 thực tế.
- Lookback được giải thích bằng số liệu.
- Bảng có đúng `9.100` hàng.
- Không trùng grain.
- `make verify` báo ổn định.

## Nội dung cần ghi vào báo cáo

- Triệu chứng thiếu hàng lịch sử.
- P99 độ trễ và đơn vị đo.
- Lookback được chọn cùng lý do.
- Cơ chế filter theo max làm mất late-arriving event.
- Số hàng và checksum trước/sau.
