WITH revenue AS (
  SELECT coalesce(sum(revenue_cents),0)::BIGINT AS revenue_cents FROM report.revenue_ledger
  WHERE month>=(SELECT period_end-INTERVAL '12 months' FROM _context)
), power AS (
  SELECT purpose,sum(contracted_mwh_capacity) AS contracted_mwh_capacity,
         sum(energized_mwh_capacity) AS energized_mwh_capacity,
         sum(consumed_mwh) AS consumed_mwh,sum(cost_cents)::BIGINT AS cost_cents
  FROM report.compute_ledger WHERE month>=(SELECT period_end-INTERVAL '12 months' FROM _context) GROUP BY purpose
), measured AS (
  SELECT p.*,c.asof AS month,r.revenue_cents,
         date_diff('hour',c.period_end-INTERVAL '12 months',c.period_end) AS hours,
         c.first_month<=c.period_end-INTERVAL '12 months' AS complete_history
  FROM power p CROSS JOIN revenue r CROSS JOIN _context c
)
SELECT month,purpose,CASE WHEN complete_history THEN 'defined' ELSE 'insufficient_history' END AS status,
       CASE WHEN purpose='inference' AND complete_history THEN revenue_cents END AS t12m_net_revenue_cents,
       cost_cents,contracted_mwh_capacity/hours AS average_contracted_mw,
       energized_mwh_capacity/hours AS average_energized_mw,consumed_mwh/hours AS utilized_mw,
       consumed_mwh/nullif(energized_mwh_capacity,0) AS energized_utilization,
       CASE WHEN purpose='inference' AND complete_history THEN revenue_cents*hours/100.0/nullif(contracted_mwh_capacity,0) END AS revenue_per_contracted_mw,
       CASE WHEN purpose='inference' AND complete_history THEN revenue_cents*hours/100.0/nullif(consumed_mwh,0) END AS revenue_per_utilized_mw
FROM measured ORDER BY purpose
