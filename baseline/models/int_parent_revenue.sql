select ultimate_parent,month,sum(net_revenue_cents)::bigint revenue_cents,
 sum(consumption_cents)::bigint consumption_cents,sum(subscription_cents)::bigint subscription_cents
from {{ref('int_customer_revenue')}} group by 1,2
