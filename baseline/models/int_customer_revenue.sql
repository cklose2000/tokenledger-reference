select r.customer,c.ultimate_parent,r.month,sum(r.revenue_cents)::bigint net_revenue_cents,
 sum(case when r.recognition_kind='usage' then r.revenue_cents else 0 end)::bigint consumption_cents,
 sum(case when r.recognition_kind='subscription' then r.revenue_cents else 0 end)::bigint subscription_cents
from {{ref('fact_revenue')}} r join {{ref('dim_customer')}} c using(customer) group by 1,2,3
