WITH parent_month AS (
  SELECT ultimate_parent,month,sum(consumption_cents)::BIGINT AS consumption_cents,
         sum(subscription_cents)::BIGINT AS subscription_cents
  FROM report.customer_month GROUP BY 1,2
), windows AS (
  SELECT l.lens,l.floor_cents,l.annualization,l.months,l.include_subscription,p.ultimate_parent,
    sum(CASE WHEN p.month>=c.period_end-INTERVAL '12 months'-l.months*INTERVAL '1 month'
                  AND p.month<c.period_end-INTERVAL '12 months'
             THEN p.consumption_cents+CASE WHEN l.include_subscription THEN p.subscription_cents ELSE 0 END ELSE 0 END)::BIGINT AS base_cents,
    sum(CASE WHEN p.month>=c.period_end-l.months*INTERVAL '1 month'
             THEN p.consumption_cents+CASE WHEN l.include_subscription THEN p.subscription_cents ELSE 0 END ELSE 0 END)::BIGINT AS current_cents
  FROM _nrr_lenses l CROSS JOIN _context c CROSS JOIN parent_month p GROUP BY 1,2,3,4,5,6
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
  FROM _nrr_lenses l LEFT JOIN eligible e USING(lens) GROUP BY 1,2,3,4
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
FROM totals t CROSS JOIN _context c ORDER BY lens
