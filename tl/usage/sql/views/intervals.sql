CREATE VIEW usage_intervals AS
WITH ordered AS (
 SELECT *,lag(ts) OVER w AS previous_ts,lag(quantity) OVER w AS previous_quantity
 FROM usage_observations CROSS JOIN _usage_window
 WHERE temporality='cumulative' AND ts<end_utc
 WINDOW w AS (PARTITION BY account,source,scope_id,token_kind ORDER BY ts,source_line)
)
SELECT *,CASE WHEN previous_ts IS NULL THEN 'opening_balance'
 WHEN previous_ts<start_utc THEN 'crosses_window_boundary'
 WHEN quantity IS NULL OR previous_quantity IS NULL THEN 'missing_counter'
 WHEN quantity<previous_quantity THEN 'counter_reset'
 ELSE 'observed' END AS interval_status,
 CASE WHEN previous_ts>=start_utc AND quantity>=previous_quantity
 THEN quantity::HUGEINT-previous_quantity::HUGEINT ELSE NULL END AS observed_tokens
FROM ordered WHERE ts>=start_utc;
