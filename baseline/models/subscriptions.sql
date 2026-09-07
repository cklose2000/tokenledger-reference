with combined as(select coalesce(s.month,t.month) AS month,coalesce(s.plan,t.plan) plan,
{% for field in ['beginning_seats','paid_seats','gross_adds','cancelled_seats','seat_removals','plan_transfers_net','net_adds','daily_weighted_paid_seats','daily_weighted_paid_accounts','beginning_paid_accounts','paid_accounts','upgraded_accounts','downgraded_accounts','expanded_accounts','revenue_cents'] %}
coalesce(s.{{field}},0) {{field}},
{% endfor %}
coalesce(t.trials_started,0) trials_started,coalesce(t.trial_cohort_converted,0) trial_cohort_converted,
coalesce(t.trial_cohort_resolved,0) trial_cohort_resolved
from {{ref('int_subscription_summary')}} s full join {{ref('int_trial_cohorts')}} t using(month,plan)), rates as(
select *, (cancelled_seats+seat_removals)::double/nullif(beginning_seats,0) gross_churn_monthly from combined)
select *,case when gross_churn_monthly between 0 and 1 then 1-power(1-gross_churn_monthly,12) end gross_churn_annualized,
(cancelled_seats+seat_removals)/nullif(daily_weighted_paid_seats,0) gross_churn_daily_exposure_lens,
revenue_cents/100.0/nullif(daily_weighted_paid_seats,0) arpu,revenue_cents/100.0/nullif(daily_weighted_paid_accounts,0) arpa,
upgraded_accounts::double/nullif(beginning_paid_accounts,0) upgrade_rate,
downgraded_accounts::double/nullif(beginning_paid_accounts,0) downgrade_rate,
expanded_accounts::double/nullif(beginning_paid_accounts,0) seat_expansion_account_rate,
trial_cohort_converted::double/nullif(trial_cohort_resolved,0) trial_conversion_resolved_cohort_rate,
trials_started-trial_cohort_resolved trial_cohort_pending from rates
