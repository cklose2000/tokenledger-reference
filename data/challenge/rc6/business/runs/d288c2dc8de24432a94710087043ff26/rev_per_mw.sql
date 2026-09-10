WITH RECURSIVE v_invoices AS MATERIALIZED (SELECT activity_id,ts,customer,activity,_source,
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
v_recognition_policy AS (SELECT * FROM (VALUES ('usage','usage','consumption'),('commit_drawdown','usage','consumption'),('subscription','subscription','subscription'),('usage','credit','credit'),('commit_drawdown','commit_expiry','commit_expiry')) AS params("source","recognition_kind","family")),
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
v_revenue_amounts AS (SELECT r.*,p.family,c.ultimate_parent,c.segment
              FROM v_recognized r JOIN v_recognition_policy p USING(source,recognition_kind)
              JOIN v_customer_identity c USING(customer)),
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
  AND c.ts<=s.ts AND c.start_date<=s.ts::DATE AND c.end_date>s.ts::DATE)
SELECT * FROM (WITH revenue AS (
  SELECT coalesce(sum(revenue_cents),0)::BIGINT AS revenue_cents FROM v_revenue_amounts
  WHERE month>=(SELECT period_end-INTERVAL '12 months' FROM v_context)
), power AS (
  SELECT purpose,sum(contracted_mwh_capacity) AS contracted_mwh_capacity,
         sum(energized_mwh_capacity) AS energized_mwh_capacity,
         sum(consumed_mwh) AS consumed_mwh,sum(cost_cents)::BIGINT AS cost_cents
  FROM v_compute_ledger WHERE month>=(SELECT period_end-INTERVAL '12 months' FROM v_context) GROUP BY purpose
), measured AS (
  SELECT p.*,c.asof AS month,r.revenue_cents,
         date_diff('hour',c.period_end-INTERVAL '12 months',c.period_end) AS hours,
         c.first_month<=c.period_end-INTERVAL '12 months' AS complete_history
  FROM power p CROSS JOIN revenue r CROSS JOIN v_context c
)
SELECT month,purpose,CASE WHEN complete_history THEN 'defined' ELSE 'insufficient_history' END AS status,
       CASE WHEN purpose='inference' AND complete_history THEN revenue_cents END AS t12m_net_revenue_cents,
       cost_cents,contracted_mwh_capacity/hours AS average_contracted_mw,
       energized_mwh_capacity/hours AS average_energized_mw,consumed_mwh/hours AS utilized_mw,
       consumed_mwh/nullif(energized_mwh_capacity,0) AS energized_utilization,
       CASE WHEN purpose='inference' AND complete_history THEN revenue_cents*hours/100.0/nullif(contracted_mwh_capacity,0) END AS revenue_per_contracted_mw,
       CASE WHEN purpose='inference' AND complete_history THEN revenue_cents*hours/100.0/nullif(consumed_mwh,0) END AS revenue_per_utilized_mw
FROM measured ORDER BY purpose
) AS requested_output