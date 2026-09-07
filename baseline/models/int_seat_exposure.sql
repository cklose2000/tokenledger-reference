select _source,customer,subscription_id,plan,month,max(days) AS days,sum(seats*exposure_days)::bigint seat_days,
 sum(case when seats>0 then exposure_days else 0 end)::bigint account_days,
 sum(daily_rate_numerator*seats*exposure_days)::hugeint price_days
from {{ref('int_subscription_exposure')}} group by 1,2,3,4,5
