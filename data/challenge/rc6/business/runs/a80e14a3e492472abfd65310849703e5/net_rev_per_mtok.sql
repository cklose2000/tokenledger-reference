WITH RECURSIVE v_usage AS (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"usage_batch_id": "VARCHAR", "model": "VARCHAR", "token_type": "VARCHAR", "channel": "VARCHAR", "workload": "VARCHAR", "tokens": "BIGINT", "effective_price_per_mtok": "DOUBLE"}') AS f
            FROM _visible WHERE activity IN ('tokens_processed')),
v_invoices AS MATERIALIZED (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"invoice_id": "VARCHAR", "usage_batch_id": "VARCHAR", "contract_id": "VARCHAR", "period_start": "DATE", "period_end": "DATE", "gross_usd": "DECIMAL(18,2)", "discount_usd": "DECIMAL(18,2)", "net_usd": "DECIMAL(18,2)", "channel_fee_usd": "DECIMAL(18,2)", "channel": "VARCHAR"}') AS f
            FROM _visible WHERE activity IN ('usage_invoiced')),
v_contracts AS MATERIALIZED (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"contract_id": "VARCHAR", "commit_usd": "DECIMAL(18,2)", "channel": "VARCHAR", "start": "DATE", "end": "DATE"}') AS f
            FROM _visible WHERE activity IN ('contract_signed','contract_renewed')),
v_funding AS MATERIALIZED (WITH ordered AS (
      SELECT i.*,c.activity_id AS contract_activity,c.f.commit_usd*100 AS commit_cents,
        c.f.start AS contract_start,c.f.end AS contract_end,
        (i.f.net_usd*100)::BIGINT AS net_cents,(i.f.channel_fee_usd*100)::BIGINT AS fee_cents,
        coalesce(sum(i.f.net_usd*100) OVER(PARTITION BY i._source,i.f.contract_id
          ORDER BY i.f.period_end,i.activity_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS prior_cents
      FROM v_invoices i LEFT JOIN v_contracts c ON i._source=c._source AND i.f.contract_id=c.f.contract_id
      WHERE i.f.contract_id IS NOT NULL
      UNION ALL SELECT i.*,NULL::VARCHAR AS contract_activity,NULL::DECIMAL(18,2) AS commit_cents,
        NULL::DATE AS contract_start,NULL::DATE AS contract_end,
        (i.f.net_usd*100)::BIGINT AS net_cents,(i.f.channel_fee_usd*100)::BIGINT AS fee_cents,
        0::HUGEINT AS prior_cents FROM v_invoices i WHERE i.f.contract_id IS NULL)
      SELECT *,least(net_cents,greatest(coalesce(commit_cents,0)-prior_cents,0))::BIGINT AS draw_cents,
        CASE WHEN contract_activity IS NULL THEN [activity_id]
             ELSE list_sort([activity_id,contract_activity]) END AS upstream_ids FROM ordered),
v_calendar AS MATERIALIZED (SELECT month::DATE AS month,(month+INTERVAL '1 month')::DATE AS month_end,
                date_diff('day',month,month+INTERVAL '1 month')::BIGINT AS days
                FROM range(DATE '2024-02-01',DATE '2026-08-01',INTERVAL '1 month') r(month)),
v_invoice_months AS (SELECT i.*,m.month,
      date_diff('day',i.f.period_start,i.f.period_end)::HUGEINT AS days,
      date_diff('day',i.f.period_start,greatest(i.f.period_start,m.month))::HUGEINT AS lo,
      date_diff('day',i.f.period_start,least(i.f.period_end,m.month_end))::HUGEINT AS hi
      FROM v_funding i JOIN v_calendar m ON i.f.period_start<m.month_end AND i.f.period_end>m.month),
v_invoice_amounts AS MATERIALIZED (SELECT *, (((2*net_cents::HUGEINT*hi+days)//(2*days))-((2*net_cents::HUGEINT*lo+days)//(2*days)))::BIGINT AS net,(((2*fee_cents::HUGEINT*hi+days)//(2*days))-((2*fee_cents::HUGEINT*lo+days)//(2*days)))::BIGINT AS fee,(((2*draw_cents::HUGEINT*hi+days)//(2*days))-((2*draw_cents::HUGEINT*lo+days)//(2*days)))::BIGINT AS draw FROM v_invoice_months),
v_expiries AS (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"contract_id": "VARCHAR"}') AS f
            FROM _visible WHERE activity IN ('contract_expired')),
v_expiry_amounts AS (WITH spent AS (
      SELECT _source,f.contract_id AS contract_id,sum(draw_cents)::BIGINT AS spent,max(f.period_end) AS last_service
      FROM v_funding GROUP BY 1,2)
      SELECT e.*,date_trunc('month',e.ts)::DATE AS month,c.f.channel AS channel,c.activity_id AS contract_activity,
        (c.f.commit_usd*100-coalesce(s.spent,0))::BIGINT AS amount,
        list_sort([e.activity_id,c.activity_id]) AS upstream_ids,s.last_service
      FROM v_expiries e LEFT JOIN v_contracts c ON e._source=c._source AND e.f.contract_id=c.f.contract_id
      LEFT JOIN spent s ON e._source=s._source AND e.f.contract_id=s.contract_id),
v_credits AS (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"credit_id": "VARCHAR", "invoice_id": "VARCHAR", "period": "DATE", "amount_usd": "DECIMAL(18,2)"}') AS f
            FROM _visible WHERE activity IN ('credit_issued')),
v_credit_amounts AS (SELECT c.*,i.activity_id AS invoice_activity,i.f.channel AS channel,
      (c.f.amount_usd*100)::BIGINT AS amount,list_sort([c.activity_id,i.activity_id]) AS upstream_ids
      FROM v_credits c LEFT JOIN v_invoices i ON c._source=i._source AND c.f.invoice_id=i.f.invoice_id),
v_subscriptions AS (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"subscription_id": "VARCHAR", "plan": "VARCHAR", "seats": "BIGINT", "seats_after": "BIGINT", "price_per_seat": "DECIMAL(18,2)", "period_end": "DATE", "billing_period": "VARCHAR"}') AS f
            FROM _visible WHERE activity IN ('subscription_started','subscription_renewed','subscription_upgraded','subscription_downgraded','subscription_cancelled','seat_added','seat_removed')),
v_context AS MATERIALIZED (SELECT * FROM (VALUES (DATE '2026-07-31',DATE '2026-08-01',DATE '2024-02-01')) AS params("asof","period_end","first_month")),
v_subscription_states AS (WITH events AS (
      SELECT *,CASE WHEN activity IN ('seat_added','seat_removed') THEN 20
        WHEN activity IN ('subscription_upgraded','subscription_downgraded') THEN 10
        WHEN activity='subscription_cancelled' THEN 30 ELSE 0 END AS priority
      FROM v_subscriptions)
      SELECT *,ts::DATE AS start_day,
        least(lead(ts::DATE,1,(SELECT period_end FROM v_context)) OVER w,
          coalesce(last_value(f.period_end IGNORE NULLS) OVER w,(SELECT period_end FROM v_context)),
          (SELECT period_end FROM v_context)) AS stop_day,
        coalesce(last_value(f.price_per_seat IGNORE NULLS) OVER w*100,0)::BIGINT AS price_cents,
        CASE WHEN activity='subscription_cancelled' THEN 0 ELSE coalesce(f.seats_after,f.seats) END AS paid_seats
      FROM events WINDOW w AS (PARTITION BY _source,customer,f.subscription_id ORDER BY ts,priority,activity_id)),
v_subscription_months AS (SELECT s._source,s.customer,s.f.subscription_id AS subscription_id,m.month,
       sum(s.price_cents::HUGEINT*s.paid_seats*date_diff('day',greatest(s.start_day,m.month),least(s.stop_day,m.month_end))) AS numerator,
       max(m.days)::HUGEINT AS days,list_sort(list_distinct(list(s.activity_id))) AS upstream_ids
      FROM v_subscription_states s JOIN v_calendar m ON s.start_day<m.month_end AND s.stop_day>m.month
      GROUP BY 1,2,3,4),
v_subscription_amounts AS (SELECT *,
      'subscription:'||subscription_id||':'||month::VARCHAR AS activity_id,
      ((2*abs(numerator)+days)//(2*days)*sign(numerator))::BIGINT AS amount FROM v_subscription_months),
v_recognized AS (WITH earnings AS (
      SELECT activity_id,_source,customer,month,'commit_drawdown' AS source,'usage' AS recognition_kind,
        f.invoice_id AS source_document,f.channel AS channel,draw AS revenue_cents,upstream_ids FROM v_invoice_amounts
      UNION ALL SELECT activity_id,_source,customer,month,'usage','usage',f.invoice_id,f.channel,net-fee-draw,upstream_ids FROM v_invoice_amounts
      UNION ALL SELECT activity_id,_source,customer,month,'commit_drawdown','commit_expiry',f.contract_id,channel,amount,upstream_ids FROM v_expiry_amounts
      UNION ALL SELECT activity_id,_source,customer,f.period,'usage','credit',f.credit_id,channel,-amount,upstream_ids FROM v_credit_amounts
      UNION ALL SELECT activity_id,_source,customer,month,'subscription','subscription',subscription_id,'direct',amount,upstream_ids FROM v_subscription_amounts)
      SELECT activity_id||':'||source||':'||recognition_kind||':'||month::VARCHAR AS activity_id,
        _source,customer,month,source,recognition_kind,source_document,channel,revenue_cents::BIGINT AS revenue_cents,upstream_ids
      FROM earnings WHERE revenue_cents<>0 AND month<(SELECT period_end FROM v_context)),
v_compute_ledger AS (-- Contract/month capacity rows plus appended meter rows. Measures do not duplicate.
WITH contracts AS (
  SELECT activity_id, _source, ts, json_extract_string(feature_json,'$.capacity_id') AS capacity_id,
         json_extract_string(feature_json,'$.provider') AS provider,
         json_extract_string(feature_json,'$.region') AS region,
         json_extract_string(feature_json,'$.purpose') AS purpose,
         CAST(json_extract_string(feature_json,'$.mw') AS DECIMAL(24,9)) AS mw,
         CAST(json_extract_string(feature_json,'$.usd_per_mwh') AS DECIMAL(18,2)) AS usd_per_mwh,
         CAST(json_extract_string(feature_json,'$.start') AS DATE) AS start_date,
         CAST(json_extract_string(feature_json,'$.end') AS DATE) AS end_date
  FROM _visible WHERE activity='capacity_contracted'
), energized AS (
  SELECT _source, json_extract_string(feature_json,'$.capacity_id') AS capacity_id, ts,
         CAST(json_extract_string(feature_json,'$.mw') AS DECIMAL(24,9)) AS mw,
         lead(ts,1,(SELECT period_end::TIMESTAMPTZ FROM v_context)) OVER
           (PARTITION BY _source,json_extract_string(feature_json,'$.capacity_id') ORDER BY ts,activity_id) AS next_ts
  FROM _visible WHERE activity='capacity_energized'
), available AS (
  SELECT c._source,c.capacity_id,m.month,
         CAST(sum(e.mw*greatest(0,date_diff('second',greatest(e.ts,m.month::TIMESTAMPTZ,c.start_date::TIMESTAMPTZ),
             least(e.next_ts,m.month_end::TIMESTAMPTZ,c.end_date::TIMESTAMPTZ))))/3600.0 AS DECIMAL(38,9)) AS energized_mwh_capacity
  FROM contracts c CROSS JOIN v_calendar m
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
  FROM _visible WHERE activity='capacity_consumed'
)
SELECT 'capacity' AS row_kind,c.activity_id,c._source,c.capacity_id,m.month,c.provider,c.region,c.purpose,
       NULL::VARCHAR AS model,
       c.mw*greatest(0,date_diff('hour',greatest(c.start_date,m.month),least(c.end_date,m.month_end))) AS contracted_mwh_capacity,
       coalesce(a.energized_mwh_capacity,0) AS energized_mwh_capacity,
       0::DECIMAL(28,9) AS consumed_mwh,0::BIGINT AS cost_cents
FROM contracts c CROSS JOIN v_calendar m
LEFT JOIN available a ON a._source=c._source AND a.capacity_id=c.capacity_id AND a.month=m.month
WHERE c.start_date<m.month_end AND c.end_date>m.month
UNION ALL
SELECT 'consumption',s.activity_id,s._source,s.capacity_id,date_trunc('month',s.month)::DATE,
       s.provider,s.region,s.purpose,s.model,0,0,s.mwh,
       CAST(round(s.mwh*c.usd_per_mwh*100) AS BIGINT)
FROM meters s JOIN contracts c ON c._source=s._source AND c.capacity_id=s.capacity_id
  AND c.provider=s.provider AND c.region=s.region AND c.purpose=s.purpose
  AND c.ts<=s.ts AND c.start_date<=s.ts::DATE AND c.end_date>s.ts::DATE),
v_allocation AS (SELECT * FROM (VALUES ('synthetic-g1','input',20000000),('synthetic-g1','output',80000000),('synthetic-g1','cache_read',3000000),('synthetic-g1','cache_write',25000000),('synthetic-g2','input',16000000),('synthetic-g2','output',64000000),('synthetic-g2','cache_read',2400000),('synthetic-g2','cache_write',20000000),('synthetic-g3','input',12800000),('synthetic-g3','output',51200000),('synthetic-g3','cache_read',1920000),('synthetic-g3','cache_write',16000000),('synthetic-g4','input',10240000),('synthetic-g4','output',40960000),('synthetic-g4','cache_read',1536000),('synthetic-g4','cache_write',12800000),('synthetic-g5','input',8192000),('synthetic-g5','output',32768000),('synthetic-g5','cache_read',1228800),('synthetic-g5','cache_write',10240000)) AS params("model","token_type","coefficient_nano")),
v_customer_identity AS (-- Primary customer creation; append latest known relationships and commercial state.
WITH RECURSIVE created AS (
  SELECT customer, ts AS created_at, activity_id,
         json_extract_string(feature_json,'$.segment') AS segment,
         json_extract_string(feature_json,'$.channel') AS channel,
         json_extract_string(feature_json,'$.country') AS country,
         json_extract_string(feature_json,'$.parent_customer') AS parent
  FROM _visible WHERE activity='customer_created'
), changes AS (
  SELECT customer, ts, activity_id, json_extract_string(feature_json,'$.ultimate_parent') AS parent
  FROM _visible WHERE activity='customer_merged'
  UNION ALL SELECT customer, created_at, activity_id, parent FROM created
), edges AS (
  SELECT customer, parent FROM changes
  QUALIFY row_number() OVER (PARTITION BY customer ORDER BY ts DESC, activity_id DESC)=1
), walk(customer,node,path) AS (
  SELECT customer, customer, [customer] FROM created
  UNION ALL
  SELECT w.customer, e.parent, list_append(w.path,e.parent)
  FROM walk w JOIN edges e ON e.customer=w.node
  WHERE e.parent IS NOT NULL AND NOT list_contains(w.path,e.parent)
), parents AS (
  SELECT w.customer, w.node AS ultimate_parent FROM walk w
  LEFT JOIN edges e ON e.customer=w.node WHERE e.parent IS NULL
)
SELECT c.customer,c.segment,c.channel,c.country,p.ultimate_parent
FROM created c JOIN parents p USING(customer)),
v_token_ledger AS (-- One token event; typed projection and exact largest-remainder allocations.
WITH tokens AS (
 SELECT activity_id,_source,customer,ts,date_trunc('month',ts)::DATE AS month,
   f.usage_batch_id AS usage_batch_id,f.model AS model,f.token_type AS token_type,
   f.channel AS channel,f.workload AS workload,f.tokens AS tokens,
   f.effective_price_per_mtok AS effective_price_per_mtok,
   greatest(0,CAST(round(f.tokens::DOUBLE*f.effective_price_per_mtok/10000) AS BIGINT)) AS rated_cents
 FROM v_usage
), models AS (
  SELECT json_extract_string(feature_json,'$.model') AS model,
         CAST(json_extract_string(feature_json,'$.generation') AS INTEGER) AS generation,
         CAST(json_extract_string(feature_json,'$.release_date') AS DATE) AS release_date,ts AS released_at
  FROM _visible WHERE activity='model_released'
), invoices AS (
 SELECT activity_id,_source,f.usage_batch_id AS usage_batch_id,f.invoice_id AS invoice_id,
   (f.net_usd*100)::BIGINT AS net_cents,(f.channel_fee_usd*100)::BIGINT AS fee_cents FROM v_invoices
), earned AS (
  SELECT _source,source_document AS invoice_id,month,sum(revenue_cents)::BIGINT AS revenue_cents
  FROM v_recognized WHERE recognition_kind='usage' GROUP BY 1,2,3
), fees AS (
    SELECT activity_id,month,fee AS month_fee_cents FROM v_invoice_amounts
  ), cost AS (
  SELECT month,model,sum(cost_cents)::BIGINT AS cost_cents FROM v_compute_ledger
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
  JOIN v_allocation a ON a.model=t.model AND a.token_type=t.token_type
  JOIN v_customer_identity c USING(customer)
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
FROM allocated)
SELECT * FROM (SELECT month,model,generation,token_type,channel,workload,segment,sum(tokens)::BIGINT AS tokens,
       sum(net_revenue_cents)::BIGINT AS net_revenue_cents,
       sum(channel_fee_cents)::BIGINT AS channel_fee_cents,
       sum(net_revenue_cents+channel_fee_cents)::BIGINT AS revenue_before_channel_fees_cents,
       sum(net_revenue_cents)*10000.0/nullif(sum(tokens),0) AS net_of_discounts_and_fees_per_mtok,
       sum(net_revenue_cents+channel_fee_cents)*10000.0/nullif(sum(tokens),0) AS net_of_discounts_per_mtok
FROM v_token_ledger GROUP BY month,model,generation,token_type,channel,workload,segment ORDER BY month,model,generation,token_type,channel,workload,segment
) AS requested_output