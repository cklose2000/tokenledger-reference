-- One row per recognized event, preserving cents before analytical allocations.
WITH invoices AS (
  SELECT _source, json_extract_string(feature_json,'$.invoice_id') AS invoice_id,
         json_extract_string(feature_json,'$.usage_batch_id') AS usage_batch_id,
         json_extract_string(feature_json,'$.channel') AS channel
  FROM _visible WHERE activity='usage_invoiced'
), credits AS (
  SELECT _source, json_extract_string(feature_json,'$.credit_id') AS credit_id,
         json_extract_string(feature_json,'$.invoice_id') AS invoice_id
  FROM _visible WHERE activity='credit_issued'
), usage_attributes AS (
 SELECT _source,f.usage_batch_id AS usage_batch_id,
   CASE WHEN count(DISTINCT f.model)=1 THEN min(f.model) ELSE 'unallocated' END AS model,
   CASE WHEN count(DISTINCT f.workload)=1 THEN min(f.workload) ELSE 'unallocated' END AS workload
 FROM @usage GROUP BY 1,2
), recognized AS (
 SELECT activity_id,_source,customer,month::TIMESTAMPTZ AS ts,month,source,
 recognition_kind,source_document,channel,revenue_cents FROM @recognized
)
SELECT r.*, p.family, c.segment, c.ultimate_parent,
       i.usage_batch_id, coalesce(u.model,'unallocated') AS model,
       coalesce(u.workload,'unallocated') AS workload
FROM recognized r JOIN @recognition_policy p USING(source,recognition_kind)
JOIN @customer_identity c USING(customer)
LEFT JOIN credits cr ON r._source=cr._source AND CASE WHEN r.recognition_kind='credit' THEN r.source_document END=cr.credit_id
LEFT JOIN invoices i ON r._source=i._source AND i.invoice_id=
  CASE WHEN r.recognition_kind='credit' THEN cr.invoice_id WHEN r.recognition_kind='usage' THEN r.source_document END
LEFT JOIN usage_attributes u ON i._source=u._source AND i.usage_batch_id=u.usage_batch_id
