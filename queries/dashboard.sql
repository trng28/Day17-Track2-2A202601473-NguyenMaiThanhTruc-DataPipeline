-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- NHIỆM VỤ 4: dataset nguồn đã được tools/compact.py ghi lại vào
-- data/gold_events_v2/, partition theo event_date (hive_partitioning), sort
-- theo (customer_name, event_time), row_group_size = 2.048.
--
-- Predicate ngày dùng thẳng cột partition `event_date` — không còn bọc
-- event_time trong strftime(...) — nên engine prune được partition trước
-- khi mở file, và filter customer_name có thể dùng min/max của row group.

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
where customer_name = 'ACME'
  and event_date = '2026-08-09'
group by 1
