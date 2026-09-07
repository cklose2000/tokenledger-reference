select month,model,generation,token_type,channel,workload,segment,
 sum(tokens)::bigint tokens,sum(allocated_cost_cents)::bigint allocated_inference_cost_cents,
 sum(net_revenue_cents)::bigint net_revenue_cents,
 10000.0*sum(allocated_cost_cents)/nullif(sum(tokens),0) cost_per_mtok,
 1-sum(allocated_cost_cents)::double/nullif(sum(net_revenue_cents),0) inference_gross_margin
from {{ref('fact_usage')}} group by 1,2,3,4,5,6,7
