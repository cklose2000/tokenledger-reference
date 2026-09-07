WITH quarters AS (
  SELECT month_end-INTERVAL '1 day' AS quarter_end,month_end AS period_end FROM _calendar
  WHERE extract(month FROM month) IN (3,6,9,12)
    AND month_end-INTERVAL '12 months'>=(SELECT first_month FROM _context)
), revenue AS (
  SELECT q.quarter_end,c.ultimate_parent,sum(c.net_revenue_cents)::BIGINT AS revenue_cents
  FROM quarters q JOIN report.customer_month c ON c.month>=q.period_end-INTERVAL '12 months' AND c.month<q.period_end
  GROUP BY 1,2
)
SELECT q.quarter_end::DATE AS month,t.threshold_cents,
       count(r.ultimate_parent) FILTER(WHERE r.revenue_cents>t.threshold_cents)::BIGINT AS customers,
       coalesce(sum(r.revenue_cents) FILTER(WHERE r.revenue_cents>t.threshold_cents),0)::BIGINT AS cohort_revenue_cents,
       coalesce(sum(r.revenue_cents),0)::BIGINT AS total_revenue_cents,
       coalesce(sum(r.revenue_cents) FILTER(WHERE r.revenue_cents>t.threshold_cents),0)::DOUBLE/nullif(sum(r.revenue_cents),0) AS net_revenue_share
FROM quarters q CROSS JOIN _cohort_thresholds t LEFT JOIN revenue r USING(quarter_end)
GROUP BY 1,2 ORDER BY 1,2
