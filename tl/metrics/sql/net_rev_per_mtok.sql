SELECT month{cuts},sum(tokens)::BIGINT AS tokens,
       sum(net_revenue_cents)::BIGINT AS net_revenue_cents,
       sum(channel_fee_cents)::BIGINT AS channel_fee_cents,
       sum(net_revenue_cents+channel_fee_cents)::BIGINT AS revenue_before_channel_fees_cents,
       sum(net_revenue_cents)*10000.0/nullif(sum(tokens),0) AS net_of_discounts_and_fees_per_mtok,
       sum(net_revenue_cents+channel_fee_cents)*10000.0/nullif(sum(tokens),0) AS net_of_discounts_per_mtok
FROM report.token_ledger GROUP BY month{cuts} ORDER BY month{cuts}
