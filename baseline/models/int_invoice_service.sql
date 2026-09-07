select i.*,d.month,d.days,
 date_diff('day',i.service_start,greatest(i.service_start,d.month)) lo_days,
 date_diff('day',i.service_start,least(i.service_end,d.month_end)) hi_days,
 date_diff('day',i.service_start,i.service_end) service_days
from {{ref('int_invoice_funding')}} i join {{ref('dim_date')}} d
on i.service_start<d.month_end and i.service_end>d.month
