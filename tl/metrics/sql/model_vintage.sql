WITH models AS (
  SELECT json_extract_string(feature_json,'$.model') AS model,
         CAST(json_extract_string(feature_json,'$.generation') AS INTEGER) AS generation,
         CAST(json_extract_string(feature_json,'$.release_date') AS DATE) AS release_date
  FROM _snapshot WHERE activity='model_released'
), revenue AS (
  SELECT month,model,generation,release_date,channel,workload,net_revenue_cents AS revenue_cents
  FROM report.token_ledger
  UNION ALL
  SELECT r.month,r.model,m.generation,m.release_date,r.channel,r.workload,r.revenue_cents
  FROM report.revenue_ledger r LEFT JOIN models m USING(model) WHERE r.family!='consumption'
), grouped AS (
  SELECT month,model,generation,release_date{cuts},sum(revenue_cents)::BIGINT AS revenue_cents
  FROM revenue GROUP BY month,model,generation,release_date{cuts}
)
SELECT month,model,generation,date_diff('month',release_date,month) AS months_since_release{cuts},revenue_cents,
       revenue_cents::DOUBLE/nullif(sum(revenue_cents) OVER(PARTITION BY month{cuts}),0) AS revenue_share,
       sum(CASE WHEN release_date>=(month+INTERVAL '1 month')-INTERVAL '12 months' THEN revenue_cents ELSE 0 END)
         OVER(PARTITION BY month{cuts})::DOUBLE/nullif(sum(revenue_cents) OVER(PARTITION BY month{cuts}),0) AS released_last_12_months_share
FROM grouped ORDER BY month,model{cuts}
