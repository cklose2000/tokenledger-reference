-- One token event. Largest-remainder allocation preserves invoice/model-month cents.
WITH tokens AS (
  SELECT s.activity_id,s._source,s.customer,s.ts,date_trunc('month',s.ts)::DATE AS month,
         json_extract_string(s.feature_json,'$.usage_batch_id') AS usage_batch_id,
         json_extract_string(s.feature_json,'$.model') AS model,
         json_extract_string(s.feature_json,'$.token_type') AS token_type,
         json_extract_string(s.feature_json,'$.channel') AS channel,
         json_extract_string(s.feature_json,'$.workload') AS workload,
         CAST(json_extract_string(s.feature_json,'$.tokens') AS BIGINT) AS tokens,
         CAST(json_extract_string(s.feature_json,'$.effective_price_per_mtok') AS DOUBLE) AS effective_price_per_mtok,
         greatest(0,CAST(round(CAST(json_extract_string(s.feature_json,'$.tokens') AS DOUBLE)
           *CAST(json_extract_string(s.feature_json,'$.effective_price_per_mtok') AS DOUBLE)/10000) AS BIGINT)) AS rated_cents
  FROM _snapshot s WHERE s.activity='tokens_processed'
), models AS (
  SELECT json_extract_string(feature_json,'$.model') AS model,
         CAST(json_extract_string(feature_json,'$.generation') AS INTEGER) AS generation,
         CAST(json_extract_string(feature_json,'$.release_date') AS DATE) AS release_date,ts AS released_at
  FROM _snapshot WHERE activity='model_released'
), invoices AS (
  SELECT _source,json_extract_string(feature_json,'$.usage_batch_id') AS usage_batch_id,
         json_extract_string(feature_json,'$.invoice_id') AS invoice_id,
         CAST(CAST(json_extract_string(feature_json,'$.net_usd') AS DECIMAL(18,2))*100 AS BIGINT) AS net_cents,
         CAST(CAST(json_extract_string(feature_json,'$.channel_fee_usd') AS DECIMAL(18,2))*100 AS BIGINT) AS fee_cents
  FROM _snapshot WHERE activity='usage_invoiced'
), earned AS (
  SELECT _source,source_document AS invoice_id,sum(revenue_cents)::BIGINT AS revenue_cents
  FROM report.revenue_ledger WHERE family='consumption' GROUP BY 1,2
), cost AS (
  SELECT month,model,sum(cost_cents)::BIGINT AS cost_cents FROM report.compute_ledger
  WHERE row_kind='consumption' AND purpose='inference' GROUP BY 1,2
), joined AS (
  SELECT t.*,m.generation,m.release_date,c.segment,c.ultimate_parent,
         coalesce(e.revenue_cents,0) AS invoice_revenue_cents,
         coalesce(CAST(((2*i.fee_cents::HUGEINT*abs(e.revenue_cents)+(i.net_cents-i.fee_cents))
           //nullif(2*(i.net_cents-i.fee_cents),0))*sign(e.revenue_cents) AS BIGINT),0) AS invoice_fee_cents,
         coalesce(k.cost_cents,0) AS model_cost_cents,
         t.tokens::HUGEINT*a.coefficient_nano AS energy_weight,
         sum(t.rated_cents) OVER (PARTITION BY t._source,t.usage_batch_id) AS total_rated,
         sum(t.tokens) OVER (PARTITION BY t._source,t.usage_batch_id) AS total_tokens,
         sum(t.tokens::HUGEINT*a.coefficient_nano) OVER (PARTITION BY t.month,t.model) AS total_energy
  FROM tokens t JOIN models m ON t.model=m.model AND t.ts>=m.released_at
  JOIN _allocation a ON a.model=t.model AND a.token_type=t.token_type
  JOIN report.customer_timeline c USING(customer)
  LEFT JOIN invoices i ON t._source=i._source AND t.usage_batch_id=i.usage_batch_id
  LEFT JOIN earned e ON e._source=i._source AND e.invoice_id=i.invoice_id
  LEFT JOIN cost k ON k.month=t.month AND k.model=t.model
), shares AS (
  SELECT *,CASE WHEN total_rated>0 THEN rated_cents ELSE tokens END AS revenue_weight,
           CASE WHEN total_rated>0 THEN total_rated ELSE total_tokens END AS revenue_denominator
  FROM joined
), fractional AS (
  SELECT *,abs(invoice_revenue_cents)::HUGEINT*revenue_weight AS rev_product,
           abs(invoice_fee_cents)::HUGEINT*revenue_weight AS fee_product,
           model_cost_cents::HUGEINT*energy_weight AS cost_product
  FROM shares
), allocated AS (
  SELECT *,(rev_product//revenue_denominator)::BIGINT AS rev_floor,(fee_product//revenue_denominator)::BIGINT AS fee_floor,
           (cost_product//total_energy)::BIGINT AS cost_floor,
    row_number() OVER(PARTITION BY _source,usage_batch_id ORDER BY rev_product%revenue_denominator DESC,activity_id) AS rev_rank,
    row_number() OVER(PARTITION BY _source,usage_batch_id ORDER BY fee_product%revenue_denominator DESC,activity_id) AS fee_rank,
    row_number() OVER(PARTITION BY month,model ORDER BY cost_product%total_energy DESC,activity_id) AS cost_rank
  FROM fractional
)
SELECT activity_id,_source,customer,ts,month,model,generation,release_date,token_type,channel,workload,segment,ultimate_parent,
       usage_batch_id,tokens,effective_price_per_mtok,energy_weight,
       sign(invoice_revenue_cents)*(rev_floor+CASE WHEN rev_rank<=abs(invoice_revenue_cents)-sum(rev_floor) OVER(PARTITION BY _source,usage_batch_id) THEN 1 ELSE 0 END) AS net_revenue_cents,
       sign(invoice_fee_cents)*(fee_floor+CASE WHEN fee_rank<=abs(invoice_fee_cents)-sum(fee_floor) OVER(PARTITION BY _source,usage_batch_id) THEN 1 ELSE 0 END) AS channel_fee_cents,
       cost_floor+CASE WHEN cost_rank<=model_cost_cents-sum(cost_floor) OVER(PARTITION BY month,model) THEN 1 ELSE 0 END AS allocated_cost_cents
FROM allocated
