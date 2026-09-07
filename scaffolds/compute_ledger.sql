-- Contract/month capacity rows plus appended meter rows. Measures do not duplicate.
WITH contracts AS (
  SELECT activity_id, _source, ts, json_extract_string(feature_json,'$.capacity_id') AS capacity_id,
         json_extract_string(feature_json,'$.provider') AS provider,
         json_extract_string(feature_json,'$.region') AS region,
         json_extract_string(feature_json,'$.purpose') AS purpose,
         CAST(json_extract_string(feature_json,'$.mw') AS DECIMAL(24,9)) AS mw,
         CAST(json_extract_string(feature_json,'$.usd_per_mwh') AS DECIMAL(18,2)) AS usd_per_mwh,
         CAST(json_extract_string(feature_json,'$.start') AS DATE) AS start_date,
         CAST(json_extract_string(feature_json,'$.end') AS DATE) AS end_date
  FROM _snapshot WHERE activity='capacity_contracted'
), energized AS (
  SELECT _source, json_extract_string(feature_json,'$.capacity_id') AS capacity_id, ts,
         CAST(json_extract_string(feature_json,'$.mw') AS DECIMAL(24,9)) AS mw,
         lead(ts,1,(SELECT period_end::TIMESTAMPTZ FROM _context)) OVER
           (PARTITION BY _source,json_extract_string(feature_json,'$.capacity_id') ORDER BY ts,activity_id) AS next_ts
  FROM _snapshot WHERE activity='capacity_energized'
), available AS (
  SELECT c._source,c.capacity_id,m.month,
         CAST(sum(e.mw*greatest(0,date_diff('second',greatest(e.ts,m.month::TIMESTAMPTZ,c.start_date::TIMESTAMPTZ),
             least(e.next_ts,m.month_end::TIMESTAMPTZ,c.end_date::TIMESTAMPTZ))))/3600.0 AS DECIMAL(38,9)) AS energized_mwh_capacity
  FROM contracts c CROSS JOIN _calendar m
  JOIN energized e ON e._source=c._source AND e.capacity_id=c.capacity_id
  GROUP BY 1,2,3
), meters AS (
  SELECT activity_id,_source,ts,json_extract_string(feature_json,'$.capacity_id') AS capacity_id,
         json_extract_string(feature_json,'$.provider') AS provider,
         json_extract_string(feature_json,'$.region') AS region,
         json_extract_string(feature_json,'$.purpose') AS purpose,
         json_extract_string(feature_json,'$.model') AS model,
         CAST(json_extract_string(feature_json,'$.mwh') AS DECIMAL(28,9)) AS mwh,
         CAST(json_extract_string(feature_json,'$.period_start') AS DATE) AS month
  FROM _snapshot WHERE activity='capacity_consumed'
)
SELECT 'capacity' AS row_kind,c.activity_id,c._source,c.capacity_id,m.month,c.provider,c.region,c.purpose,
       NULL::VARCHAR AS model,
       c.mw*greatest(0,date_diff('hour',greatest(c.start_date,m.month),least(c.end_date,m.month_end))) AS contracted_mwh_capacity,
       coalesce(a.energized_mwh_capacity,0) AS energized_mwh_capacity,
       0::DECIMAL(28,9) AS consumed_mwh,0::BIGINT AS cost_cents
FROM contracts c CROSS JOIN _calendar m
LEFT JOIN available a ON a._source=c._source AND a.capacity_id=c.capacity_id AND a.month=m.month
WHERE c.start_date<m.month_end AND c.end_date>m.month
UNION ALL
SELECT 'consumption',s.activity_id,s._source,s.capacity_id,date_trunc('month',s.month)::DATE,
       s.provider,s.region,s.purpose,s.model,0,0,s.mwh,
       CAST(round(s.mwh*c.usd_per_mwh*100) AS BIGINT)
FROM meters s JOIN contracts c ON c._source=s._source AND c.capacity_id=s.capacity_id
  AND c.provider=s.provider AND c.region=s.region AND c.purpose=s.purpose
  AND c.ts<=s.ts AND c.start_date<=s.ts::DATE AND c.end_date>s.ts::DATE
