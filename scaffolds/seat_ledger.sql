-- Paid-state intervals clipped to half-open calendar months; trials are separate.
WITH events AS (
  SELECT activity_id,_source,customer,ts,activity,
         json_extract_string(feature_json,'$.subscription_id') AS subscription_id,
         json_extract_string(feature_json,'$.plan') AS plan,
         CASE WHEN activity='subscription_cancelled' THEN 0
              WHEN activity IN ('seat_added','seat_removed') THEN CAST(json_extract_string(feature_json,'$.seats_after') AS BIGINT)
              ELSE CAST(json_extract_string(feature_json,'$.seats') AS BIGINT) END AS seats,
         CAST(json_extract_string(feature_json,'$.price_per_seat') AS DECIMAL(18,2)) AS price,
         CASE WHEN activity IN ('seat_added','seat_removed') THEN 20
              WHEN activity IN ('subscription_upgraded','subscription_downgraded') THEN 10
              WHEN activity='subscription_cancelled' THEN 30 ELSE 0 END AS priority
  FROM _snapshot WHERE activity IN ('subscription_started','subscription_renewed','subscription_upgraded',
       'subscription_downgraded','subscription_cancelled','seat_added','seat_removed')
), states AS (
  SELECT *,ts::DATE AS effective_day,
         lead(ts::DATE,1,(SELECT period_end FROM _context)) OVER w AS next_day,
         lag(seats,1,0) OVER w AS previous_seats,
         lag(plan,1,plan) OVER w AS previous_plan,
         last_value(price IGNORE NULLS) OVER w AS effective_price
  FROM events WINDOW w AS (PARTITION BY _source,customer,subscription_id ORDER BY ts,priority,activity_id)
), exposures AS (
  SELECT s._source,s.customer,s.subscription_id,s.plan,m.month,
         sum(s.seats*date_diff('day',greatest(s.effective_day,m.month),least(s.next_day,m.month_end))) AS seat_days,
         sum(CASE WHEN s.seats>0 THEN date_diff('day',greatest(s.effective_day,m.month),least(s.next_day,m.month_end)) ELSE 0 END) AS account_days,
         sum(s.seats*s.effective_price*100*date_diff('day',greatest(s.effective_day,m.month),least(s.next_day,m.month_end))) AS price_seat_cent_days
  FROM states s JOIN _calendar m ON s.effective_day<m.month_end AND s.next_day>m.month
  GROUP BY 1,2,3,4,5
), points AS (
  SELECT s._source,s.customer,s.subscription_id,s.plan,m.month,
         sum(CASE WHEN s.effective_day<m.month AND s.next_day>=m.month THEN s.seats ELSE 0 END) AS beginning_seats,
         sum(CASE WHEN s.effective_day<m.month_end AND s.next_day>=m.month_end THEN s.seats ELSE 0 END) AS ending_seats
  FROM states s JOIN _calendar m ON s.effective_day<m.month_end AND s.next_day>=m.month
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
  FROM report.revenue_ledger WHERE family='subscription' GROUP BY 1,2,3,4
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
  FROM keys k JOIN _calendar m USING(month)
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
FROM allocated
