# Task 05 - Consumer chịu được sự cố giữa batch

> Bài mở rộng, tối đa cộng 5 điểm.

## Mục tiêu

Đảm bảo consumer không mất dữ liệu khi process chết giữa batch và không tạo bản ghi trùng khi batch được replay.

## Hiện tượng

Consumer commit offset trước khi ghi dữ liệu. Nếu process chết sau commit nhưng trước write, lần restart bỏ qua batch chưa được lưu và gây mất dữ liệu.

## Yêu cầu bắt buộc

- `make crash-test` đạt.
- Không mất event khi consumer restart.
- Replay cùng `event_id` không tạo thêm hàng.
- Phép ghi xử lý đúng khi nội dung event replay đã thay đổi.
- `make verify` của ba task chính vẫn đạt.

## File cần chỉnh sửa

- `ingest/consumer.py`

Chỉ đọc, không cần sửa `ingest/log_client.py`.

## Các bước thực hiện

### Bước 1 - Tái hiện sự cố

```bash
make crash-test
```

Ghi lại:

- Số message nguồn.
- Số hàng đích sau crash và restart.
- Số event bị mất hoặc bị trùng.
- Batch nơi process bị kill.

### Bước 2 - Vẽ timeline hiện tại

Phân tích ba thao tác:

```text
poll batch
commit offset
crash point
write batch
```

Tại crash point, trả lời:

- Dữ liệu đã được ghi chưa?
- Offset đã được lưu chưa?
- Restart bắt đầu từ offset nào?

Đây là cơ chế at-most-once và có nguy cơ mất dữ liệu.

### Bước 3 - Chuyển sang at-least-once

Sắp lại thứ tự để chỉ commit sau khi batch đã được ghi thành công. Giữ crash point ở vị trí giúp test mô phỏng process chết sau write nhưng trước commit.

Khi đó restart có thể đọc lại batch, nên cần hoàn thành bước idempotent write bên dưới.

### Bước 4 - Tạo khóa chống trùng

Xác định khóa tự nhiên của event là `event_id`. Thêm ràng buộc `PRIMARY KEY` hoặc `UNIQUE` phù hợp trong DDL để DuckDB cho phép `ON CONFLICT`.

### Bước 5 - Biến write thành idempotent

Thay insert thuần bằng upsert:

```text
INSERT ... ON CONFLICT (event_id) DO ...
```

Chọn hành vi khi cùng event được replay:

- `DO NOTHING`: giữ bản đầu tiên.
- `DO UPDATE`: cập nhật dữ liệu mới nhất.

Đối chiếu yêu cầu nghiệp vụ và giải thích lựa chọn. Nếu message cùng ID có nội dung được sửa, `DO NOTHING` và `DO UPDATE` cho kết quả khác nhau.

### Bước 6 - Kiểm tra không crash

Chạy consumer bình thường và xác nhận số hàng bằng số event duy nhất trong topic.

### Bước 7 - Kiểm tra crash và restart

```bash
make crash-test
```

Test phải xác nhận:

- Không mất hàng.
- Không trùng `event_id`.
- Offset cuối cùng được commit đầy đủ.

### Bước 8 - Kiểm tra hồi quy

```bash
make verify
```

## Tiêu chí hoàn thành

- `make crash-test` báo đạt.
- `count(*) = count(distinct event_id)`.
- Số hàng đích bằng số event nguồn duy nhất.
- Thứ tự write trước, commit sau.
- Phép ghi là idempotent.

## Nội dung cần ghi vào báo cáo

- Trạng thái trước sửa là at-most-once hay at-least-once.
- Điểm crash làm mất hoặc replay batch như thế nào.
- Vì sao transport không tự cung cấp exactly-once end-to-end.
- Cách kết hợp at-least-once với idempotent write.
- Khác biệt giữa `DO UPDATE` và `DO NOTHING`.
- Kết quả crash test trước/sau.
