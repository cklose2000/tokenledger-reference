select u.* exclude(revenue_cents,fee_cents),r.allocated_cents net_revenue_cents,f.allocated_cents channel_fee_cents,c.allocated_cents allocated_cost_cents
from {{ref('int_usage_weights')}} u
join {{ref('int_usage_revenue_allocation')}} r using(activity_id)
join {{ref('int_usage_fee_allocation')}} f using(activity_id)
join {{ref('int_usage_cost_allocation')}} c using(activity_id)
