# Kết quả LAB 17 - Data Pipeline Engineering

Tài liệu này lưu bằng chứng thực thi cho từng nhiệm vụ. Trạng thái chỉ được chuyển sang
`Đạt` khi kết quả kiểm tra đáp ứng đầy đủ tiêu chí trong `RUBRIC.md`.

## Task 01 - Khắc phục bảng training không idempotent

**Trạng thái:** Chưa đạt, đã tái hiện lỗi baseline.

### Lệnh kiểm tra

```powershell
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python tools/run_pipeline.py
python tools/verify.py
```

### Kết quả quan sát

| Tiêu chí | Kết quả hiện tại | Kỳ vọng | Trạng thái |
|---|---:|---:|---|
| Số hàng `gold_training_set` sau ba lượt | 38.750 | 12.480 | Chưa đạt |
| Số hàng thừa | 26.270 | 0 | Chưa đạt |
| Ticket bị lặp | 12.480 | 0 | Chưa đạt |
| Checksum ổn định qua ba lượt | Không | Có | Chưa đạt |

Checksum của `gold_training_set` thay đổi ở cả ba lượt chạy. Điều này xác nhận model
incremental đang ghi thêm dữ liệu cũ khi pipeline được chạy lại, thay vì cập nhật theo
grain một hàng trên mỗi `ticket_id`.

### Bằng chứng

![Kết quả baseline Task 01](assets/task-1.png)

### File cần xử lý

- `dbt/models/gold/gold_training_set.sql`
- `dags/ai_training_pipeline.py`

### Điều kiện chuyển sang Đạt

- `gold_training_set` có đúng 12.480 hàng.
- Không có `ticket_id` trùng.
- Checksum giống nhau qua ba lượt `verify` và các lượt chạy bổ sung.
- Cấu hình DAG đạt kiểm tra `catchup` và `max_active_runs`.

Sau khi hoàn tất sửa source, chạy lại `python tools/verify.py` và thay ảnh baseline bằng
bằng chứng mới hoặc bổ sung ảnh kết quả sau sửa vào mục này.

---

## Task 02 - Xử lý event đến muộn

**Trạng thái:** Chưa đạt, đã sinh dữ liệu kiểm thử, tái hiện lỗi và đo độ trễ.

### Lệnh chuẩn bị và kiểm tra

```powershell
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python seed/generate.py --extra
python tools/verify.py --runs 1
```

### Dữ liệu kiểm thử

| Thành phần | Số lượng |
|---|---:|
| CDC rows | 14.300 |
| Events | 130.683 |
| Transcripts | 5.200 |
| Parquet files | 5.000 |
| `gold_feature_daily` kỳ vọng | 9.100 |

### Kết quả baseline

| Tiêu chí | Kết quả hiện tại | Kỳ vọng | Trạng thái |
|---|---:|---:|---|
| Số hàng `gold_feature_daily` | 8.645 | 9.100 | Chưa đạt |
| Số hàng thiếu | 455 | 0 | Chưa đạt |
| Ổn định trong lượt quick | Có | Có | Đạt một phần |

Bảng ổn định nhưng sai số hàng. Điều này cho thấy incremental filter đang bỏ qua event
đến muộn ở các ngày đã có trong bảng đích; chạy lại cùng logic không thể tự bổ sung các
partition lịch sử đã bị bỏ sót.

### Phân bố độ trễ đã đo

Độ trễ được tính bằng `_ingested_at - event_time` trên `bronze_events`.

| Chỉ số | Giá trị |
|---|---:|
| P50 | 11.067 giây |
| P95 | 156.703,05 giây |
| P99 | 235.512 giây, tương đương 2,7258 ngày |
| Max | 254.421 giây, tương đương 2,9447 ngày |
| Event trễ hơn 1 ngày | 5,0509% |

Lookback được đề xuất là **3 ngày**, bằng P99 làm tròn lên. Cửa sổ tính lại phải đi kèm
composite key theo grain `(event_date, customer_id)` và chiến lược ghi thay thế để không
tạo hàng trùng.

### Bằng chứng

![Sinh dữ liệu mở rộng cho Task 02](assets/task-2-generate.png)

![Kết quả baseline Task 02](assets/task-2-verify.png)

### File cần xử lý

- `dbt/models/gold/gold_feature_daily.sql`

### Điều kiện chuyển sang Đạt

- `gold_feature_daily` có đúng 9.100 hàng.
- Không trùng grain `(event_date, customer_id)`.
- Checksum giống nhau qua ba lượt `python tools/verify.py`.
- Báo cáo giữ lại P99 đo được và giải thích lựa chọn lookback 3 ngày.

Sau khi sửa model, cần bổ sung ảnh verify ba lượt đạt để thay thế hoặc đặt cạnh ảnh
baseline hiện tại.
