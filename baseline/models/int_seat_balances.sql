select h._source,h.customer,h.subscription_id,h.plan,d.month,
 sum(case when h.valid_from<d.month and h.valid_to>=d.month then h.seats else 0 end)::bigint beginning_seats,
 sum(case when h.valid_from<d.month_end and h.valid_to>=d.month_end then h.seats else 0 end)::bigint ending_seats
from {{ref('int_subscription_history')}} h join {{ref('dim_date')}} d on h.valid_from<d.month_end and h.valid_to>=d.month
group by 1,2,3,4,5
