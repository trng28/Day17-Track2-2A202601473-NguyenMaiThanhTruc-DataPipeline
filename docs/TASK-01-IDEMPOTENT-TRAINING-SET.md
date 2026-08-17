# Task 01 - Khắc phục bảng training không idempotent

## Mục tiêu

Đảm bảo `gold_training_set` luôn có đúng một hàng cho mỗi `ticket_id` và không tăng số hàng khi pipeline được retry hoặc chạy lại cùng ngày.

## Hiện tượng

Sau khi Airflow Clear Task hoặc pipeline chạy lại, `gold_training_set` tiếp tục ghi thêm các hàng đã tồn tại. Pipeline không báo lỗi nhưng dữ liệu training bị lặp.

## Yêu cầu bắt buộc

- Grain: `1 hàng / 1 ticket`.
- Số hàng cuối cùng: `12.480`.
- Không có `ticket_id` trùng.
- Checksum không thay đổi qua ít nhất ba lần chạy.
- DAG không tự backfill ngoài ý muốn và không chạy chồng nhiều run.
- Giữ nguyên bộ lọc `run_date` trong model.

## File cần phân tích và chỉnh sửa

- `dbt/models/gold/gold_training_set.sql`
- `dags/ai_training_pipeline.py`

Không cần cài hoặc chạy Airflow. `make verify` kiểm tra cấu hình DAG bằng AST.

## Các bước thực hiện

### Bước 1 - Tái hiện và ghi baseline

```bash
make reset
make pipeline
```

Đếm số hàng và số khóa phân biệt:

```sql
select count(*) as rows,
       count(distinct ticket_id) as tickets
from gold_training_set;
```

Chạy lại `make pipeline`, thực hiện lại query và ghi số liệu trước/sau.

### Bước 2 - Xác nhận grain và khóa tự nhiên

Đọc phần đầu của `gold_training_set.sql` và trả lời:

1. Đây là bảng entity hay bảng event?
2. Cột nào nhận diện duy nhất một hàng?
3. Một ticket có thể có bản ghi CDC `c` và `u` hay không?
4. Khi ticket được cập nhật, hàng cũ phải được thay thế hay giữ lại?

### Bước 3 - Kiểm tra cấu hình incremental

Trong `config()`, đối chiếu ba thuộc tính:

- `materialized`
- `unique_key`
- `incremental_strategy`

Xác định SQL mà dbt sinh ra khi thiếu khóa. Có thể xem SQL đã compile tại:

```text
dbt/target/run/lab17/models/gold/gold_training_set.sql
```

Chọn chiến lược ghi phù hợp với bảng entity có cập nhật. Cấu hình phải khiến lần chạy sau cập nhật cùng entity thay vì append thêm hàng.

### Bước 4 - Giữ nguyên phạm vi run date

Không xóa điều kiện `_ingested_at` theo `run_date`. Điều kiện này cho phép backfill một ngày cụ thể mà không quét toàn bộ lịch sử. Lỗi nằm ở cách ghi vào bảng đích, không nằm ở filter này.

### Bước 5 - Kiểm tra DAG

Đọc `catchup` và `max_active_runs` trong `dags/ai_training_pipeline.py`.

Thiết lập để:

- Scheduler không tự tạo toàn bộ historical runs ngoài ý muốn.
- Chỉ có một run pipeline hoạt động tại một thời điểm.
- Retry hoặc Clear Task không gây nhiều run ghi chồng lên cùng bảng.

### Bước 6 - Kiểm tra nhanh

```bash
make quick
```

Kiểm tra thêm:

```sql
select ticket_id, count(*) as n
from gold_training_set
group by ticket_id
having count(*) > 1;
```

Query phải trả về 0 hàng.

### Bước 7 - Kiểm tra tính idempotent

```bash
make verify
```

Sau đó chạy thêm hai lần:

```bash
make pipeline
make pipeline
```

Số hàng và checksum không được thay đổi.

## Tiêu chí hoàn thành

- `gold_training_set`: `12.480` hàng.
- `count(*) = count(distinct ticket_id)`.
- Cột `ỔN ĐỊNH` trong `make verify` là đạt.
- Kiểm tra DAG về `catchup` và `max_active_runs` đạt.
- Task 02 và Task 03 không bị ảnh hưởng.

## Nội dung cần ghi vào báo cáo

- Triệu chứng và số hàng qua các lần chạy.
- Cơ chế gây lặp, không chỉ tên tham số đã sửa.
- File và cấu hình đã thay đổi.
- Số hàng, số khóa phân biệt và checksum sau sửa.
