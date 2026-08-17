# Hướng dẫn nhiệm vụ LAB 17

Thư mục này tách yêu cầu trong `README.md`, `GUIDE.md`, `RUBRIC.md`, `EXTRA.md` và các khối `KHUNG THỰC HIỆN` thành tài liệu thao tác cho từng nhiệm vụ.

## Thứ tự thực hiện

1. [Task 01 - Khắc phục bảng training không idempotent](TASK-01-IDEMPOTENT-TRAINING-SET.md)
2. [Task 02 - Xử lý event đến muộn](TASK-02-LATE-ARRIVING-EVENTS.md)
3. [Task 03 - Data contract và quarantine](TASK-03-DATA-CONTRACT-QUARANTINE.md)
4. [Task 04 - Tối ưu dashboard Parquet](TASK-04-PARQUET-OPTIMIZATION.md), không bắt buộc
5. [Task 05 - Consumer chịu được sự cố giữa batch](TASK-05-CONSUMER-DELIVERY-SEMANTICS.md), không bắt buộc

## Chuẩn bị chung

Chạy trong Git Bash, WSL hoặc môi trường có GNU Make vì `Makefile` sử dụng `/bin/bash` và đường dẫn `.venv/bin/*`.

```bash
make setup
make pipeline
make verify
```

Quy trình cho mỗi task:

```text
Tái hiện lỗi -> đo baseline -> xác định nguyên nhân -> sửa tối thiểu
-> kiểm tra nhanh -> kiểm tra hồi quy -> ghi bằng chứng vào báo cáo
```

## File không được sửa

- `expected/*`
- `seed/generate.py`
- `tools/verify.py`
- `tools/explain.py`
- `tools/common.py`

Không xóa dữ liệu nguồn để làm cho số hàng khớp kỳ vọng.

## Kết quả cuối cùng

```bash
make verify
make dbt-test
```

Ba lần chạy phải có checksum ổn định. Trước khi nộp, chạy:

```bash
make clean
```

Không nộp `.venv/`, `warehouse.duckdb`, `dbt/target`, `dbt/logs` hoặc dữ liệu crash tạm thời.
