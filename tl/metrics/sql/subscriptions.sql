WITH seats AS (
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
  FROM report.seat_ledger GROUP BY 1,2
), trial_outcomes AS (
  SELECT _source,json_extract_string(feature_json,'$.trial_id') AS trial_id,
         max(CASE WHEN activity='trial_converted' THEN 1 ELSE 0 END) AS converted,
         max(CASE WHEN activity IN ('trial_converted','trial_expired') THEN 1 ELSE 0 END) AS resolved
  FROM _snapshot WHERE activity IN ('trial_converted','trial_expired') GROUP BY 1,2
), trials AS (
  SELECT date_trunc('month',s.ts)::DATE AS month,json_extract_string(s.feature_json,'$.plan') AS plan,
         count(*)::BIGINT AS trials_started,coalesce(sum(o.converted),0)::BIGINT AS trial_cohort_converted,
         coalesce(sum(o.resolved),0)::BIGINT AS trial_cohort_resolved
  FROM _snapshot s LEFT JOIN trial_outcomes o ON o._source=s._source
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
