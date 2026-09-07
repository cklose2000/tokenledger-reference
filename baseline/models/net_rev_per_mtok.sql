select month,model,generation,token_type,channel,workload,segment,
 sum(tokens)::bigint tokens,sum(net_revenue_cents)::bigint net_revenue_cents,
 sum(channel_fee_cents)::bigint channel_fee_cents,sum(net_revenue_cents+channel_fee_cents)::bigint revenue_before_channel_fees_cents,
 10000.0*sum(net_revenue_cents)/nullif(sum(tokens),0) net_of_discounts_and_fees_per_mtok,
 10000.0*sum(net_revenue_cents+channel_fee_cents)/nullif(sum(tokens),0) net_of_discounts_per_mtok
from {{ref('fact_usage')}} group by 1,2,3,4,5,6,7
