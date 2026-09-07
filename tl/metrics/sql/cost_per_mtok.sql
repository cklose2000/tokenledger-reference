SELECT month{cuts},sum(tokens)::BIGINT AS tokens,
       sum(allocated_cost_cents)::BIGINT AS allocated_inference_cost_cents,
       sum(net_revenue_cents)::BIGINT AS net_revenue_cents,
       sum(allocated_cost_cents)*10000.0/nullif(sum(tokens),0) AS cost_per_mtok,
       1-sum(allocated_cost_cents)::DOUBLE/nullif(sum(net_revenue_cents),0) AS inference_gross_margin
FROM report.token_ledger GROUP BY month{cuts} ORDER BY month{cuts}
