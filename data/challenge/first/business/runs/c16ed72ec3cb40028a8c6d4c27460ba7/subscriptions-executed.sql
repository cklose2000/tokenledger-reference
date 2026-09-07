WITH RECURSIVE v_context AS MATERIALIZED (SELECT * FROM (VALUES (DATE '2026-07-31',DATE '2026-08-01',DATE '2024-02-01')) AS params("asof","period_end","first_month")),
v_calendar AS MATERIALIZED (SELECT month::DATE AS month,(month+INTERVAL '1 month')::DATE AS month_end,
                date_diff('day',month,month+INTERVAL '1 month')::BIGINT AS days
                FROM range(DATE '2024-02-01',DATE '2026-08-01',INTERVAL '1 month') r(month)),
v_usage AS (SELECT activity_id,ts,customer,activity,_source,
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
v_subscription_amounts AS MATERIALIZED (SELECT *,
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
v_revenue_ledger AS (-- One row per recognized event, preserving cents before analytical allocations.
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
 FROM v_usage GROUP BY 1,2
), recognized AS (
 SELECT activity_id,_source,customer,month::TIMESTAMPTZ AS ts,month,source,
 recognition_kind,source_document,channel,revenue_cents FROM v_recognized
)
SELECT r.*, p.family, c.segment, c.ultimate_parent,
       i.usage_batch_id, coalesce(u.model,'unallocated') AS model,
       coalesce(u.workload,'unallocated') AS workload
FROM recognized r JOIN v_recognition_policy p USING(source,recognition_kind)
JOIN v_customer_identity c USING(customer)
LEFT JOIN credits cr ON r._source=cr._source AND CASE WHEN r.recognition_kind='credit' THEN r.source_document END=cr.credit_id
LEFT JOIN invoices i ON r._source=i._source AND i.invoice_id=
  CASE WHEN r.recognition_kind='credit' THEN cr.invoice_id WHEN r.recognition_kind='usage' THEN r.source_document END
LEFT JOIN usage_attributes u ON i._source=u._source AND i.usage_batch_id=u.usage_batch_id),
v_seat_ledger AS MATERIALIZED (-- Paid-state intervals clipped to half-open calendar months; trials are separate.
WITH events AS (
  SELECT activity_id,_source,customer,ts,activity,
         json_extract_string(feature_json,'$.subscription_id') AS subscription_id,
         json_extract_string(feature_json,'$.plan') AS plan,
         CASE WHEN activity='subscription_cancelled' THEN 0
              WHEN activity IN ('seat_added','seat_removed') THEN CAST(json_extract_string(feature_json,'$.seats_after') AS BIGINT)
              ELSE CAST(json_extract_string(feature_json,'$.seats') AS BIGINT) END AS seats,
         CAST(json_extract_string(feature_json,'$.price_per_seat') AS DECIMAL(18,2)) AS price,
         CAST(json_extract_string(feature_json,'$.period_end') AS DATE) AS service_end,
         CASE WHEN activity IN ('seat_added','seat_removed') THEN 20
              WHEN activity IN ('subscription_upgraded','subscription_downgraded') THEN 10
              WHEN activity='subscription_cancelled' THEN 30 ELSE 0 END AS priority
  FROM _visible WHERE activity IN ('subscription_started','subscription_renewed','subscription_upgraded',
       'subscription_downgraded','subscription_cancelled','seat_added','seat_removed')
), states AS MATERIALIZED (
  SELECT *,ts::DATE AS effective_day,
         least(lead(ts::DATE,1,(SELECT period_end FROM v_context)) OVER w,last_value(service_end IGNORE NULLS) OVER w) AS next_day,
         lag(seats,1,0) OVER w AS previous_seats,
         lag(plan,1,plan) OVER w AS previous_plan,
         last_value(price IGNORE NULLS) OVER w AS effective_price
  FROM events WINDOW w AS (PARTITION BY _source,customer,subscription_id ORDER BY ts,priority,activity_id)
), exposures AS (
  SELECT s._source,s.customer,s.subscription_id,s.plan,m.month,
         sum(s.seats*date_diff('day',greatest(s.effective_day,m.month),least(s.next_day,m.month_end))) AS seat_days,
         sum(CASE WHEN s.seats>0 THEN date_diff('day',greatest(s.effective_day,m.month),least(s.next_day,m.month_end)) ELSE 0 END) AS account_days,
         sum(s.seats*s.effective_price*100*date_diff('day',greatest(s.effective_day,m.month),least(s.next_day,m.month_end))) AS price_seat_cent_days
  FROM states s JOIN v_calendar m ON s.effective_day<m.month_end AND s.next_day>m.month
  GROUP BY 1,2,3,4,5
), points AS (
  SELECT s._source,s.customer,s.subscription_id,s.plan,m.month,
         sum(CASE WHEN s.effective_day<m.month AND s.next_day>=m.month THEN s.seats ELSE 0 END) AS beginning_seats,
         sum(CASE WHEN s.effective_day<m.month_end AND s.next_day>=m.month_end THEN s.seats ELSE 0 END) AS ending_seats
  FROM states s JOIN v_calendar m ON s.effective_day<m.month_end AND s.next_day>=m.month
  GROUP BY 1,2,3,4,5
), movements AS (
  SELECT _source,customer,subscription_id,plan,date_trunc('month',ts)::DATE AS month,
         greatest(seats-previous_seats,0) AS gross_adds,
         CASE WHEN activity!='subscription_cancelled' THEN greatest(previous_seats-seats,0) ELSE 0 END AS seat_removals,
         CASE WHEN activity='subscription_cancelled' THEN previous_seats ELSE 0 END AS cancelled_seats,
         CASE WHEN plan!=previous_plan THEN previous_seats ELSE 0 END AS transfer_in,0 AS transfer_out,
         0 AS upgrade_event,0 AS downgrade_event
  FROM states
  UNION ALL
  SELECT _source,customer,subscription_id,previous_plan,date_trunc('month',ts)::DATE,0,0,0,0,
         CASE WHEN plan!=previous_plan THEN previous_seats ELSE 0 END,
         CASE WHEN activity='subscription_upgraded' THEN 1 ELSE 0 END,
         CASE WHEN activity='subscription_downgraded' THEN 1 ELSE 0 END
  FROM states WHERE plan!=previous_plan OR activity IN ('subscription_upgraded','subscription_downgraded')
), flows AS (
  SELECT _source,customer,subscription_id,plan,month,sum(gross_adds) AS gross_adds,
         sum(seat_removals) AS seat_removals,sum(cancelled_seats) AS cancelled_seats,
         sum(transfer_in) AS transfer_in,sum(transfer_out) AS transfer_out,
         max(upgrade_event) AS upgraded_account,max(downgrade_event) AS downgraded_account
  FROM movements GROUP BY 1,2,3,4,5
), keys AS (
  SELECT _source,customer,subscription_id,plan,month FROM exposures UNION
  SELECT _source,customer,subscription_id,plan,month FROM points UNION
  SELECT _source,customer,subscription_id,plan,month FROM flows
), earned AS (
  SELECT _source,customer,source_document AS subscription_id,month,sum(revenue_cents)::BIGINT AS revenue_cents
  FROM v_revenue_ledger WHERE family='subscription' GROUP BY 1,2,3,4
), joined AS (
  SELECT k.*,m.days,
         coalesce(p.beginning_seats,0)::BIGINT AS beginning_seats,coalesce(p.ending_seats,0)::BIGINT AS ending_seats,
         coalesce(e.seat_days,0)::BIGINT AS seat_days,coalesce(e.account_days,0)::BIGINT AS account_days,
         coalesce(e.price_seat_cent_days,0) AS price_seat_cent_days,
         coalesce(f.gross_adds,0)::BIGINT AS gross_adds,coalesce(f.seat_removals,0)::BIGINT AS seat_removals,
         coalesce(f.cancelled_seats,0)::BIGINT AS cancelled_seats,coalesce(f.transfer_in,0)::BIGINT AS transfer_in,
         coalesce(f.transfer_out,0)::BIGINT AS transfer_out,coalesce(f.upgraded_account,0)::INTEGER AS upgraded_account,
         coalesce(f.downgraded_account,0)::INTEGER AS downgraded_account,
         coalesce(r.revenue_cents,0) AS subscription_revenue_cents
  FROM keys k JOIN v_calendar m USING(month)
  LEFT JOIN exposures e USING(_source,customer,subscription_id,plan,month)
  LEFT JOIN points p USING(_source,customer,subscription_id,plan,month)
  LEFT JOIN flows f USING(_source,customer,subscription_id,plan,month)
  LEFT JOIN earned r USING(_source,customer,subscription_id,month)
), fractional AS (
  SELECT *,abs(subscription_revenue_cents)::HUGEINT*price_seat_cent_days::HUGEINT AS revenue_product,
         (sum(price_seat_cent_days) OVER(PARTITION BY _source,customer,subscription_id,month))::HUGEINT AS revenue_denominator
  FROM joined
), allocated AS (
  SELECT *,coalesce(revenue_product//nullif(revenue_denominator,0),0)::BIGINT AS revenue_floor,
         row_number() OVER(PARTITION BY _source,customer,subscription_id,month ORDER BY revenue_product%nullif(revenue_denominator,0) DESC,plan) AS remainder_rank
  FROM fractional
)
SELECT _source,customer,subscription_id,plan,month,days,beginning_seats,ending_seats,seat_days,account_days,
       gross_adds,seat_removals,cancelled_seats,transfer_in,transfer_out,upgraded_account,downgraded_account,
       sign(subscription_revenue_cents)*(revenue_floor+CASE WHEN remainder_rank<=abs(subscription_revenue_cents)-
         sum(revenue_floor) OVER(PARTITION BY _source,customer,subscription_id,month) THEN 1 ELSE 0 END) AS revenue_cents
FROM allocated),
v_seat_errors AS (WITH seats AS MATERIALIZED (SELECT * FROM v_seat_ledger),
      earned AS (SELECT _source,customer,subscription_id,month,amount AS cents FROM v_subscription_amounts),
      allocated AS (SELECT _source,customer,subscription_id,month,sum(revenue_cents)::HUGEINT AS cents FROM seats GROUP BY 1,2,3,4)
      SELECT 'subscription_allocations_reconcile' AS reason,count(*) AS failures FROM earned a FULL JOIN allocated b USING(_source,customer,subscription_id,month)
        WHERE coalesce(a.cents,0)<>coalesce(b.cents,0)
      UNION ALL SELECT 'seat_rollforward',count(*) FROM seats WHERE beginning_seats+gross_adds-seat_removals-cancelled_seats+transfer_in-transfer_out<>ending_seats
      UNION ALL SELECT 'revenue_has_exposure',count(*) FROM seats WHERE revenue_cents<>0 AND seat_days=0
      UNION ALL SELECT 'unique_trials',count(*) FROM (SELECT _source,json_extract_string(feature_json,'$.trial_id') FROM _visible WHERE activity='trial_started' GROUP BY 1,2 HAVING count(*)>1))
SELECT * FROM (SELECT 'subscriptions' AS _population,* FROM (WITH seats AS (
  SELECT month,plan,sum(beginning_seats)::BIGINT AS beginning_seats,sum(ending_seats)::BIGINT AS paid_seats,
         sum(gross_adds)::BIGINT AS gross_adds,sum(cancelled_seats)::BIGINT AS cancelled_seats,
         sum(seat_removals)::BIGINT AS seat_removals,sum(transfer_in-transfer_out)::BIGINT AS plan_transfers_net,
         sum(ending_seats-beginning_seats)::BIGINT AS net_adds,
         sum(seat_days)::DOUBLE/max(days) AS daily_weighted_paid_seats,
         sum(account_days)::DOUBLE/max(days) AS daily_weighted_paid_accounts,
         count(DISTINCT customer) FILTER(WHERE beginning_seats>0)::BIGINT AS beginning_paid_accounts,
         count(DISTINCT customer) FILTER(WHERE ending_seats>0)::BIGINT AS paid_accounts,
         count(DISTINCT customer) FILTER(WHERE upgraded_account>0)::BIGINT AS upgraded_accounts,
         count(DISTINCT customer) FILTER(WHERE downgraded_account>0)::BIGINT AS downgraded_accounts,
         count(DISTINCT customer) FILTER(WHERE ending_seats>beginning_seats AND beginning_seats>0)::BIGINT AS expanded_accounts,
         sum(revenue_cents)::BIGINT AS revenue_cents
  FROM v_seat_ledger GROUP BY 1,2
), trial_outcomes AS (
  SELECT _source,json_extract_string(feature_json,'$.trial_id') AS trial_id,
         max(CASE WHEN activity='trial_converted' THEN 1 ELSE 0 END) AS converted,
         max(CASE WHEN activity IN ('trial_converted','trial_expired') THEN 1 ELSE 0 END) AS resolved
  FROM _visible WHERE activity IN ('trial_converted','trial_expired') GROUP BY 1,2
), trials AS (
  SELECT date_trunc('month',s.ts)::DATE AS month,json_extract_string(s.feature_json,'$.plan') AS plan,
         count(*)::BIGINT AS trials_started,coalesce(sum(o.converted),0)::BIGINT AS trial_cohort_converted,
         coalesce(sum(o.resolved),0)::BIGINT AS trial_cohort_resolved
  FROM _visible s LEFT JOIN trial_outcomes o ON o._source=s._source
    AND o.trial_id=json_extract_string(s.feature_json,'$.trial_id')
  WHERE s.activity='trial_started' GROUP BY 1,2
), combined AS (
  SELECT coalesce(s.month,t.month) AS month,coalesce(s.plan,t.plan) AS plan,
         coalesce(s.beginning_seats,0) AS beginning_seats,coalesce(s.paid_seats,0) AS paid_seats,
         coalesce(s.gross_adds,0) AS gross_adds,coalesce(s.cancelled_seats,0) AS cancelled_seats,
         coalesce(s.seat_removals,0) AS seat_removals,coalesce(s.plan_transfers_net,0) AS plan_transfers_net,
         coalesce(s.net_adds,0) AS net_adds,coalesce(s.daily_weighted_paid_seats,0) AS daily_weighted_paid_seats,
         coalesce(s.daily_weighted_paid_accounts,0) AS daily_weighted_paid_accounts,
         coalesce(s.beginning_paid_accounts,0) AS beginning_paid_accounts,coalesce(s.paid_accounts,0) AS paid_accounts,
         coalesce(s.upgraded_accounts,0) AS upgraded_accounts,coalesce(s.downgraded_accounts,0) AS downgraded_accounts,
         coalesce(s.expanded_accounts,0) AS expanded_accounts,coalesce(s.revenue_cents,0) AS revenue_cents,
         coalesce(t.trials_started,0) AS trials_started,coalesce(t.trial_cohort_converted,0) AS trial_cohort_converted,
         coalesce(t.trial_cohort_resolved,0) AS trial_cohort_resolved
  FROM seats s FULL OUTER JOIN trials t USING(month,plan)
), rates AS (
 SELECT *, (cancelled_seats+seat_removals)::DOUBLE/nullif(beginning_seats,0) AS gross_churn_monthly
 FROM combined
)
SELECT *,CASE WHEN gross_churn_monthly BETWEEN 0 AND 1 THEN 1-pow(1-gross_churn_monthly,12) END AS gross_churn_annualized,
       (cancelled_seats+seat_removals)/nullif(daily_weighted_paid_seats,0) AS gross_churn_daily_exposure_lens,
       revenue_cents/100.0/nullif(daily_weighted_paid_seats,0) AS arpu,
       revenue_cents/100.0/nullif(daily_weighted_paid_accounts,0) AS arpa,
       upgraded_accounts::DOUBLE/nullif(beginning_paid_accounts,0) AS upgrade_rate,
       downgraded_accounts::DOUBLE/nullif(beginning_paid_accounts,0) AS downgrade_rate,
       expanded_accounts::DOUBLE/nullif(beginning_paid_accounts,0) AS seat_expansion_account_rate,
       trial_cohort_converted::DOUBLE/nullif(trial_cohort_resolved,0) AS trial_conversion_resolved_cohort_rate,
       trials_started-trial_cohort_resolved AS trial_cohort_pending
FROM rates ORDER BY month,plan
) q UNION ALL BY NAME SELECT '__check__' AS _population,'seat_errors' AS _check_domain,reason,failures FROM v_seat_errors) AS requested_output