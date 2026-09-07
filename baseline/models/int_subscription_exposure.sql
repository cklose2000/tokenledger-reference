select h.*,d.month,d.days,
 date_diff('day',greatest(h.valid_from,d.month),least(h.valid_to,d.month_end)) exposure_days
from {{ref('int_subscription_history')}} h join {{ref('dim_date')}} d
on h.valid_from<d.month_end and h.valid_to>d.month
