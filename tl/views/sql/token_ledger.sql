-- One token event; typed projection and exact largest-remainder allocations.
WITH tokens AS (
 SELECT activity_id,_source,customer,ts,date_trunc('month',ts)::DATE AS month,
   f.usage_batch_id AS usage_batch_id,f.model AS model,f.token_type AS token_type,
   f.channel AS channel,f.workload AS workload,f.tokens AS tokens,
   f.effective_price_per_mtok AS effective_price_per_mtok,
   greatest(0,CAST(round(f.tokens::DOUBLE*f.effective_price_per_mtok/10000) AS BIGINT)) AS rated_cents
 FROM @usage
), models AS (
  SELECT json_extract_string(feature_json,'$.model') AS model,
         CAST(json_extract_string(feature_json,'$.generation') AS INTEGER) AS generation,
         CAST(json_extract_string(feature_json,'$.release_date') AS DATE) AS release_date,ts AS released_at
  FROM _visible WHERE activity='model_released'
), invoices AS (
 SELECT activity_id,_source,f.usage_batch_id AS usage_batch_id,f.invoice_id AS invoice_id,
   (f.net_usd*100)::BIGINT AS net_cents,(f.channel_fee_usd*100)::BIGINT AS fee_cents FROM @invoices
), earned AS (
  SELECT _source,source_document AS invoice_id,month,sum(revenue_cents)::BIGINT AS revenue_cents
  FROM @recognized WHERE recognition_kind='usage' GROUP BY 1,2,3
), fees AS (
    SELECT activity_id,month,fee AS month_fee_cents FROM @invoice_amounts
  ), cost AS (
  SELECT month,model,sum(cost_cents)::BIGINT AS cost_cents FROM @compute_ledger
  WHERE row_kind='consumption' AND purpose='inference' GROUP BY 1,2
), joined AS (
  SELECT t.*,m.generation,m.release_date,c.segment,c.ultimate_parent,
         coalesce(e.revenue_cents,0) AS invoice_revenue_cents,
         coalesce(f.month_fee_cents,0) AS invoice_fee_cents,
         coalesce(k.cost_cents,0) AS model_cost_cents,
         t.tokens::HUGEINT*a.coefficient_nano AS energy_weight,
         sum(t.rated_cents) OVER (PARTITION BY t._source,t.usage_batch_id,t.month) AS total_rated,
         sum(t.tokens) OVER (PARTITION BY t._source,t.usage_batch_id,t.month) AS total_tokens,
         sum(t.tokens::HUGEINT*a.coefficient_nano) OVER (PARTITION BY t.month,t.model) AS total_energy
  FROM tokens t JOIN models m ON t.model=m.model AND t.ts>=m.released_at
  JOIN @allocation a ON a.model=t.model AND a.token_type=t.token_type
  JOIN @customer_identity c USING(customer)
  LEFT JOIN invoices i ON t._source=i._source AND t.usage_batch_id=i.usage_batch_id
  LEFT JOIN earned e ON e._source=i._source AND e.invoice_id=i.invoice_id AND e.month=t.month
  LEFT JOIN fees f ON f.activity_id=i.activity_id AND f.month=t.month
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
    row_number() OVER(PARTITION BY _source,usage_batch_id,month ORDER BY rev_product%revenue_denominator DESC,activity_id) AS rev_rank,
    row_number() OVER(PARTITION BY _source,usage_batch_id,month ORDER BY fee_product%revenue_denominator DESC,activity_id) AS fee_rank,
    row_number() OVER(PARTITION BY month,model ORDER BY cost_product%total_energy DESC,activity_id) AS cost_rank
  FROM fractional
)
SELECT activity_id,_source,customer,ts,month,model,generation,release_date,token_type,channel,workload,segment,ultimate_parent,
       usage_batch_id,tokens,effective_price_per_mtok,energy_weight,
       sign(invoice_revenue_cents)*(rev_floor+CASE WHEN rev_rank<=abs(invoice_revenue_cents)-sum(rev_floor) OVER(PARTITION BY _source,usage_batch_id,month) THEN 1 ELSE 0 END) AS net_revenue_cents,
       sign(invoice_fee_cents)*(fee_floor+CASE WHEN fee_rank<=abs(invoice_fee_cents)-sum(fee_floor) OVER(PARTITION BY _source,usage_batch_id,month) THEN 1 ELSE 0 END) AS channel_fee_cents,
       cost_floor+CASE WHEN cost_rank<=model_cost_cents-sum(cost_floor) OVER(PARTITION BY month,model) THEN 1 ELSE 0 END AS allocated_cost_cents
FROM allocated
