with allocated as(select _source,usage_batch_id,month,sum(net_revenue_cents)::bigint revenue_cents,sum(channel_fee_cents)::bigint fee_cents
from {{ref('fact_usage')}} group by 1,2,3)
select i.activity_id from {{ref('fact_invoice')}} i left join allocated a using(_source,usage_batch_id,month)
where coalesce(a.revenue_cents,0)<>i.net_earned_cents-i.fee_earned_cents or coalesce(a.fee_cents,0)<>i.fee_earned_cents
