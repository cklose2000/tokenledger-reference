select _source,customer,subscription_id,month,
 {{cumulative('sum(daily_rate_numerator*seats*exposure_days)','1','max(days)')}} revenue_cents,
 list(distinct activity_id order by activity_id) upstream_ids
from {{ref('int_subscription_exposure')}} group by 1,2,3,4
