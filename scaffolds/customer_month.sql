-- Primary customer timeline crossed with months; append monthly activity aggregates.
WITH revenue AS (
  SELECT customer,month,sum(revenue_cents)::BIGINT AS net_revenue_cents,
         sum(CASE WHEN family='consumption' THEN revenue_cents ELSE 0 END)::BIGINT AS consumption_cents,
         sum(CASE WHEN family='subscription' THEN revenue_cents ELSE 0 END)::BIGINT AS subscription_cents,
         sum(CASE WHEN family='credit' THEN revenue_cents ELSE 0 END)::BIGINT AS credit_cents,
         sum(CASE WHEN family='commit_expiry' THEN revenue_cents ELSE 0 END)::BIGINT AS commit_expiry_cents
  FROM report.revenue_ledger GROUP BY 1,2
), usage AS (
  SELECT customer,month,sum(tokens)::BIGINT AS tokens FROM report.token_ledger GROUP BY 1,2
), seats AS (
  SELECT customer,month,sum(beginning_seats)::BIGINT AS beginning_seats,sum(ending_seats)::BIGINT AS ending_seats,
         sum(seat_days)::BIGINT AS seat_days,sum(account_days)::BIGINT AS account_days
  FROM report.seat_ledger GROUP BY 1,2
)
SELECT c.customer,c.ultimate_parent,c.segment,c.channel,m.month,m.days,
       coalesce(r.net_revenue_cents,0) AS net_revenue_cents,coalesce(r.consumption_cents,0) AS consumption_cents,
       coalesce(r.subscription_cents,0) AS subscription_cents,coalesce(r.credit_cents,0) AS credit_cents,
       coalesce(r.commit_expiry_cents,0) AS commit_expiry_cents,coalesce(t.tokens,0) AS tokens,
       coalesce(s.beginning_seats,0) AS beginning_seats,coalesce(s.ending_seats,0) AS ending_seats,
       coalesce(s.seat_days,0) AS seat_days,coalesce(s.account_days,0) AS account_days
FROM report.customer_timeline c CROSS JOIN _calendar m
LEFT JOIN revenue r ON r.customer=c.customer AND r.month=m.month
LEFT JOIN usage t ON t.customer=c.customer AND t.month=m.month
LEFT JOIN seats s ON s.customer=c.customer AND s.month=m.month
WHERE m.month_end>c.first_activity::DATE
