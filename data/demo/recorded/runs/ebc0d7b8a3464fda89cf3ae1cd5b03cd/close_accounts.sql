WITH RECURSIVE v_contracts AS MATERIALIZED (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"contract_id": "VARCHAR", "commit_usd": "DECIMAL(18,2)", "channel": "VARCHAR", "start": "DATE", "end": "DATE"}') AS f
            FROM _visible WHERE activity IN ('contract_signed','contract_renewed')),
v_invoices AS MATERIALIZED (SELECT activity_id,ts,customer,activity,_source,
            from_json(feature_json,'{"invoice_id": "VARCHAR", "usage_batch_id": "VARCHAR", "contract_id": "VARCHAR", "period_start": "DATE", "period_end": "DATE", "gross_usd": "DECIMAL(18,2)", "discount_usd": "DECIMAL(18,2)", "net_usd": "DECIMAL(18,2)", "channel_fee_usd": "DECIMAL(18,2)", "channel": "VARCHAR"}') AS f
            FROM _visible WHERE activity IN ('usage_invoiced')),
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
v_invoice_amounts AS (SELECT *, (((2*net_cents::HUGEINT*hi+days)//(2*days))-((2*net_cents::HUGEINT*lo+days)//(2*days)))::BIGINT AS net,(((2*fee_cents::HUGEINT*hi+days)//(2*days))-((2*fee_cents::HUGEINT*lo+days)//(2*days)))::BIGINT AS fee,(((2*draw_cents::HUGEINT*hi+days)//(2*days))-((2*draw_cents::HUGEINT*lo+days)//(2*days)))::BIGINT AS draw FROM v_invoice_months),
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
v_journals AS (WITH postings AS (
      SELECT c.activity_id,c._source,c.customer,date_trunc('month',c.ts)::DATE AS month,
        ['1100','2100'] AS accounts,[(c.f.commit_usd*100)::BIGINT,(-c.f.commit_usd*100)::BIGINT] AS amounts,
        [c.activity_id] AS upstream_ids FROM v_contracts c
      UNION ALL SELECT activity_id,_source,customer,month,['2100','1100','4000','4095'],
        [draw,net-draw-fee,-net,fee],upstream_ids FROM v_invoice_amounts
      UNION ALL SELECT activity_id,_source,customer,month,['2100','4020'],[amount,-amount],upstream_ids FROM v_expiry_amounts
      UNION ALL SELECT activity_id,_source,customer,f.period,['4090','1100'],[amount,-amount],upstream_ids FROM v_credit_amounts
      UNION ALL SELECT activity_id,_source,customer,month,['1100','4010'],[amount,-amount],upstream_ids FROM v_subscription_amounts),
      expanded AS (SELECT activity_id,_source,customer,month,unnest(accounts) AS account,unnest(amounts)::BIGINT AS amount_cents,upstream_ids FROM postings)
      SELECT * FROM expanded WHERE amount_cents<>0)
SELECT * FROM (SELECT month,account,sum(amount_cents)::BIGINT AS amount_cents FROM v_journals GROUP BY 1,2) AS requested_output