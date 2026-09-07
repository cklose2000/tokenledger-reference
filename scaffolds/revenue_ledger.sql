-- One row per recognized event, preserving cents before analytical allocations.
WITH invoices AS (
  SELECT _source, json_extract_string(feature_json,'$.invoice_id') AS invoice_id,
         json_extract_string(feature_json,'$.usage_batch_id') AS usage_batch_id,
         json_extract_string(feature_json,'$.channel') AS channel
  FROM _snapshot WHERE activity='usage_invoiced'
), credits AS (
  SELECT _source, json_extract_string(feature_json,'$.credit_id') AS credit_id,
         json_extract_string(feature_json,'$.invoice_id') AS invoice_id
  FROM _snapshot WHERE activity='credit_issued'
), usage_attributes AS (
  SELECT _source, json_extract_string(feature_json,'$.usage_batch_id') AS usage_batch_id,
         CASE WHEN count(DISTINCT json_extract_string(feature_json,'$.model'))=1
              THEN min(json_extract_string(feature_json,'$.model')) ELSE 'unallocated' END AS model,
         CASE WHEN count(DISTINCT json_extract_string(feature_json,'$.workload'))=1
              THEN min(json_extract_string(feature_json,'$.workload')) ELSE 'unallocated' END AS workload
  FROM _snapshot WHERE activity='tokens_processed' GROUP BY 1,2
), recognized AS (
  SELECT activity_id, _source, customer, ts, CAST(json_extract_string(feature_json,'$.period') AS DATE) AS month,
         json_extract_string(feature_json,'$.source') AS source,
         json_extract_string(feature_json,'$.recognition_kind') AS recognition_kind,
         json_extract_string(feature_json,'$.source_document') AS source_document,
         json_extract_string(feature_json,'$.channel') AS channel,
         CAST(revenue_impact*100 AS BIGINT) AS revenue_cents
  FROM _snapshot WHERE activity='revenue_recognized'
)
SELECT r.*, p.family, c.segment, c.ultimate_parent,
       i.usage_batch_id, coalesce(u.model,'unallocated') AS model,
       coalesce(u.workload,'unallocated') AS workload
FROM recognized r JOIN _recognition_policy p USING(source,recognition_kind)
JOIN report.customer_timeline c USING(customer)
LEFT JOIN credits cr ON r._source=cr._source AND r.recognition_kind='credit' AND r.source_document=cr.credit_id
LEFT JOIN invoices i ON r._source=i._source AND i.invoice_id=
  CASE WHEN r.recognition_kind='credit' THEN cr.invoice_id WHEN r.recognition_kind='usage' THEN r.source_document END
LEFT JOIN usage_attributes u ON i._source=u._source AND i.usage_batch_id=u.usage_batch_id
