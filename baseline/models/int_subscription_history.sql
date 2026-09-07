select *,ts::date valid_from,
 least(lead(ts::date,1,date '{{var("asof")}}'+1) over w,
 last_value(service_end ignore nulls) over w,date '{{var("asof")}}'+1) valid_to,
 last_value(price_cents ignore nulls) over w daily_rate_numerator,
 lag(seats,1,0) over w previous_seats,lag(plan,1,plan) over w previous_plan
from {{ref('stg_subscription_events')}}
window w as(partition by _source,customer,subscription_id order by ts,precedence,activity_id)
