# Báo cáo LAB 17 - Data Pipeline Engineering

**Họ tên:** Nguyễn Mai Thanh Trúc **MSHV:** 2A202601473  **Lớp:** E403  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```markdown
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 188.1s
  run 2/3 … 150.5s
  run 3/3 … 132.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau khi Airflow Clear Task hoặc chạy lại pipeline, `gold_training_set` ghi thêm hàng đã tồn tại thay vì cập nhật; số hàng tăng dần qua mỗi lượt chạy mà không có lỗi nào được báo. |
| **Nguyên nhân** | `config()` của model chỉ khai `materialized = 'incremental'`, không khai `unique_key`. Thiếu `unique_key`, dbt-duckdb sinh câu lệnh ghi là `INSERT` thuần. Nguồn `silver_tickets` (materialized = 'table') phản ánh trạng thái *mới nhất* của mỗi ticket tại thời điểm chạy; một ticket tạo ngày D1 rồi update ngày D2 thoả điều kiện lọc `_ingested_at` theo `run_date` ở **cả hai** lượt chạy D1 và D2 — vì phép ghi là INSERT chứ không phải upsert theo khoá, ticket đó xuất hiện hai lần trong bảng đích thay vì một lần được cập nhật. Bản thân phép ghi không idempotent, nên mọi cơ chế retry/Clear Task ở tầng trên biến thành cơ chế nhân bản. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1` (giảm tần suất kích hoạt, không phải root cause). |
| **Bằng chứng** | trước: 38.750 hàng (12.480 ticket bị lặp) · sau: 12.480 hàng cả ba lượt · checksum 3 lượt: `8dd7c98653` / `8dd7c98653` / `8dd7c98653` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định qua nhiều lần chạy (cùng checksum) nhưng thiếu ~5% hàng, tập trung ở các ngày đã "đóng" từ lâu; ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2,7258 ngày** (235.512 giây) *(bắt buộc)* P50 = 11.067s, P95 = 156.703,05s, max = 254.421s (≈2,9447 ngày), 5,0509% event trễ hơn 1 ngày. |
| **Lookback đã chọn** | **3 ngày**  P99 làm tròn lên, bao phủ 99% độ trễ quan sát được và cả phần lớn đuôi max, mà không phải quét lại toàn bộ lịch sử ở mọi lượt chạy sau này. |
| **Nguyên nhân** | Điều kiện `where event_date > (select max(event_date) from {{ this }})` chỉ nhận ngày *lớn hơn* ngày lớn nhất đã có trong đích. Một event có `event_date=08-12` nhưng `_ingested_at=08-15` không thoả điều kiện này ở lượt chạy 08-15 (vì `max(event_date)` trong đích khi đó đã ≥ 08-14), và không bao giờ thoả ở bất kỳ lượt sau đó (`max` chỉ tăng dần)  event bị bỏ sót vĩnh viễn, không phải bỏ sót tạm thời. Đây là lỗi *đúng*, không phải lỗi *ổn định*. |
| **Cách khắc phục** | Đổi filter thành `where event_date > (select max(event_date) from {{ this }}) - interval 3 day`; thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để mỗi lần tính lại cửa sổ 3 ngày **thay thế** kết quả cũ thay vì cộng dồn (tránh tái tạo lỗi mục 1 trên bảng này). |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455) · sau: 9.100 hàng cả ba lượt, checksum `3db448685c` ổn định, không trùng `(event_date, customer_id)` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là một điểm ngoại lệ đơn lẻ; dùng nó làm lookback nghĩa là MỌI lượt chạy sau
> này đều phải quét lại toàn bộ cửa sổ đó chỉ để phục vụ đúng một bản ghi hiếm gặp 
> chi phí trả lặp lại vĩnh viễn cho một trường hợp cá biệt. P99 bao phủ 99% trường hợp
> với cửa sổ hẹp hơn nhiều (3 ngày so với gần 3 ngày của max, nhưng về nguyên tắc lựa
> chọn là dựa trên phân phối chứ không phải giá trị cực trị), đổi lại chấp nhận rủi ro
> bỏ sót đúng phần đuôi 1% còn lại rủi ro này được đo lường và ghi nhận rõ ràng thay
> vì bị che giấu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10, backend đổi `priority` từ số sang nhãn chữ. Pipeline không dừng, nhưng `silver_tickets.priority` có 6.606 hàng NULL hoặc ngoài miền `1..4`; `quarantine_tickets` rỗng dù đã có 312 bản ghi lỗi thật; 9/9 dbt test gốc vẫn pass. |
| **Nguyên nhân** | `normalize_priority()` dùng `try_cast(priority_raw as integer)` sai theo **hai hướng cùng lúc**: biến nhãn chữ hợp lệ (`urgent/high/medium/low`) thành NULL (mất dữ liệu tốt), đồng thời vẫn chấp nhận số ngoài miền (`0`, `5`, `-1`) vì chúng ép kiểu số hợp lệ dù ngoài hợp đồng dữ liệu. `contract.enforced: false` nên sai kiểu không bị chặn; 9 test gốc không kiểm tra miền giá trị nên vẫn pass dù dữ liệu sai. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) Số hợp lệ `1..4` → giữ nguyên. (2) Nhãn chữ hợp lệ `urgent/high/medium/low` (schema evolution, ý nghĩa không đổi) → map về `1/2/3/4` theo tài liệu API backend. (3) Giá trị hỏng thật (`P1`, `P2`, `0`, `5`, `-1`, rỗng, NULL, `unknown`) → trả `NULL` từ macro, đưa vào quarantine. |
| **Cách khắc phục** | Viết lại `normalize_priority()` bằng `CASE` xử lý đủ ba nhóm, dùng chung cho cả `silver_tickets` và `quarantine_tickets` để hai model không lệch định nghĩa. Trong `silver_tickets`: đổi thứ tự CTE thành *chuẩn hoá → lọc bản ghi NULL → xếp hạng CDC → lấy mới nhất* (lọc **bản ghi**, không lọc **ticket**, nên vẫn giữ đủ 12.480 ticket). `quarantine_tickets`: thay `where false` bằng điều kiện macro trả NULL. `schema.yml`: bật `contract.enforced: true`, thêm test `not_null` + `accepted_values([1,2,3,4])` cho `priority`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng 312/312, khớp phân bố `reject_reason` đã khảo sát) · `dbt test` 11/11 pass (thêm 2 test so với baseline 9) · `silver_tickets` vẫn 12.480 ticket, 0 hàng priority sai |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Chặn ở Bronze sẽ làm mất payload gốc (`priority_raw`, thời điểm CDC) ngay từ đầu —
> khi cần điều tra "vì sao ticket X bị coi là lỗi" thì không còn dữ liệu để xem lại.
> Quarantine ở Silver giữ nguyên `priority_raw`, `cdc_seq`, `event_time`, `_ingested_at`
> nên điều tra sự cố về sau không bị cản trở. Về quy mô: 312 bản ghi lỗi so với hơn
> 130.000 event và 31.200 chunk hoàn toàn bình thường đang chờ phục vụ người dùng —
> dừng cả DAG vì 312 bản ghi là đánh đổi bất cân xứng. Quarantine tách phần lỗi thành
> một hàng đợi cho người trực xử lý, còn phần dữ liệu tốt tiếp tục phục vụ mô hình và
> RAG index không gián đoạn.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A — tối ưu dashboard Parquet (small-file problem) |
| **Nguyên nhân** | `data/gold_events/` gồm 5.000 file nhỏ, không partition, hàng ngẫu nhiên; đồng thời predicate ngày bọc cột trong `strftime(event_time, ...)` nên không sargable. Không có thông tin filter nào (ngày, khách hàng) nằm trong path hay trong min/max statistics hữu ích, nên engine phải mở gần như toàn bộ 5.000 file dù chỉ cần dữ liệu của 1 khách hàng trong 1 ngày. |
| **Cách khắc phục** | `tools/compact.py`: `COPY ... TO 'data/gold_events_v2'` với `PARTITION_BY (event_date)` (14 giá trị phân biệt → 14 thư mục, không partition theo `customer_name` vì 650 giá trị sẽ tái tạo small-file problem), `ORDER BY customer_name, event_time` (gom hàng cùng khách hàng liền nhau để min/max của row group hẹp lại), `ROW_GROUP_SIZE 2048` (nhỏ hơn ~9.300 hàng/ngày, để mỗi ngày có nhiều row group thay vì một row group phủ hết). `queries/dashboard.sql`: đọc dataset mới với `hive_partitioning = true`, lọc `event_date = '2026-08-09'` trực tiếp (sargable) thay vì bọc `strftime`. |
| **Bằng chứng** | rows scanned: 5.000.000 → 9.324 (giảm 536,3×, cần ≥10×) · files: 5.000 → 14 · result hash không đổi: `4379e4c5d9f3` = `4379e4c5d9f3` · số hàng nguồn = đích = 130.683 |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Với mọi model `incremental`, kiểm tra `unique_key` + `incremental_strategy` có khớp với grain thực tế (entity vs event) trước khi tin rằng "không lỗi, không dừng" nghĩa là "đúng" một bảng có thể chạy trơn tru và vẫn âm thầm nhân bản dữ liệu. |
| 2 | Đo phân bố độ trễ thực tế (`_ingested_at - event_time`) bằng percentile trước khi tin vào bất kỳ điều kiện lọc "chỉ lấy dữ liệu mới hơn X" — một filter tưởng như vô hại có thể vĩnh viễn bỏ sót dữ liệu về muộn nếu không có lookback window dựa trên số liệu đo được. |
| 3 | Khi nguồn đổi định dạng dữ liệu (schema evolution), phân loại rõ "đổi cách biểu diễn" và "dữ liệu lỗi thật" trước khi viết logic validate — gộp hai nhóm này làm một sẽ hoặc làm mất dữ liệu tốt, hoặc để lọt dữ liệu xấu, tuỳ hướng sai. |
