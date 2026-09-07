WITH RECURSIVE v_customer_identity AS MATERIALIZED (-- Primary customer creation; append latest known relationships and commercial state.
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
v_recognition_policy AS (SELECT * FROM (VALUES ('usage','usage','consumption'),('commit_drawdown','usage','consumption'),('subscription','subscription','subscription'),('usage','credit','credit'),('commit_drawdown','commit_expiry','commit_expiry')) AS params("source","recognition_kind","family")),
v_parent_month AS (SELECT c.ultimate_parent,r.month,
              sum(CASE WHEN p.family='consumption' THEN r.revenue_cents ELSE 0 END)::BIGINT AS consumption_cents,
              sum(CASE WHEN p.family='subscription' THEN r.revenue_cents ELSE 0 END)::BIGINT AS subscription_cents,
              sum(r.revenue_cents)::BIGINT AS net_revenue_cents
              FROM v_recognized r JOIN v_recognition_policy p USING(source,recognition_kind)
              JOIN v_customer_identity c USING(customer) GROUP BY 1,2),
v_nrr_lenses AS MATERIALIZED (SELECT * FROM (VALUES ('base_t12m',12,1,false,1000000),('floor_100k',12,1,false,10000000),('t3m_annualized',3,4,false,1000000),('subscription_inclusive',12,1,true,1000000)) AS params("lens","months","annualization","include_subscription","floor_cents"))
SELECT * FROM (WITH parent_month AS (
              SELECT u.ultimate_parent,p.month,coalesce(p.consumption_cents,0)::BIGINT AS consumption_cents,
                coalesce(p.subscription_cents,0)::BIGINT AS subscription_cents
              FROM (SELECT DISTINCT ultimate_parent FROM v_customer_identity) u
              LEFT JOIN v_parent_month p USING(ultimate_parent)
            ), windows AS (
  SELECT l.lens,l.floor_cents,l.annualization,l.months,l.include_subscription,p.ultimate_parent,
    sum(CASE WHEN p.month>=c.period_end-INTERVAL '12 months'-l.months*INTERVAL '1 month'
                  AND p.month<c.period_end-INTERVAL '12 months'
             THEN p.consumption_cents+CASE WHEN l.include_subscription THEN p.subscription_cents ELSE 0 END ELSE 0 END)::BIGINT AS base_cents,
    sum(CASE WHEN p.month>=c.period_end-l.months*INTERVAL '1 month'
             THEN p.consumption_cents+CASE WHEN l.include_subscription THEN p.subscription_cents ELSE 0 END ELSE 0 END)::BIGINT AS current_cents
  FROM v_nrr_lenses l CROSS JOIN v_context c CROSS JOIN parent_month p GROUP BY 1,2,3,4,5,6
), eligible AS (
  SELECT *,base_cents*annualization>=floor_cents AS in_cohort FROM windows
), totals AS (
  SELECT l.lens,l.months,l.floor_cents,l.annualization,
    count(e.ultimate_parent) FILTER(WHERE e.in_cohort)::BIGINT AS cohort_customers,
    coalesce(sum(e.base_cents),0)::BIGINT AS all_base_cents,
    coalesce(sum(e.base_cents) FILTER(WHERE e.in_cohort),0)::BIGINT AS base_revenue_cents,
    coalesce(sum(e.current_cents) FILTER(WHERE e.in_cohort),0)::BIGINT AS current_revenue_cents,
    coalesce(sum(greatest(e.current_cents-e.base_cents,0)) FILTER(WHERE e.in_cohort),0)::BIGINT AS expansion_cents,
    coalesce(sum(CASE WHEN e.current_cents!=0 THEN greatest(e.base_cents-e.current_cents,0) ELSE 0 END) FILTER(WHERE e.in_cohort),0)::BIGINT AS contraction_cents,
    coalesce(sum(CASE WHEN e.current_cents=0 THEN e.base_cents ELSE 0 END) FILTER(WHERE e.in_cohort),0)::BIGINT AS churn_cents
  FROM v_nrr_lenses l LEFT JOIN eligible e USING(lens) GROUP BY 1,2,3,4
)
SELECT c.asof AS month,t.lens,
       CASE WHEN c.first_month>c.period_end-INTERVAL '12 months'-t.months*INTERVAL '1 month' THEN 'insufficient_history'
            WHEN base_revenue_cents=0 THEN 'undefined_zero_base' ELSE 'defined' END AS status,
       cohort_customers,all_base_cents,base_revenue_cents,current_revenue_cents,expansion_cents,contraction_cents,churn_cents,
       CASE WHEN status='defined' THEN current_revenue_cents::DOUBLE/base_revenue_cents END AS nrr,
       CASE WHEN status='defined' THEN expansion_cents::DOUBLE/base_revenue_cents END AS expansion_share,
       CASE WHEN status='defined' THEN contraction_cents::DOUBLE/base_revenue_cents END AS contraction_share,
       CASE WHEN status='defined' THEN churn_cents::DOUBLE/base_revenue_cents END AS churn_share,
       CASE WHEN status='defined' THEN base_revenue_cents::DOUBLE/nullif(all_base_cents,0) END AS cohort_revenue_coverage
FROM totals t CROSS JOIN v_context c ORDER BY lens
) AS requested_output