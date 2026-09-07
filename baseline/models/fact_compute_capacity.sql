select c._source,c.capacity_id,d.month,c.provider,c.region,c.purpose,
 c.mw*greatest(0,date_diff('hour',greatest(c.service_start,d.month),least(c.service_end,d.month_end))) contracted_mwh_capacity,
 coalesce(sum(h.mw*greatest(0,date_diff('second',greatest(h.valid_from,d.month::timestamptz,c.service_start::timestamptz),
 least(h.valid_to,d.month_end::timestamptz,c.service_end::timestamptz))))/3600.0,0)::decimal(38,9) energized_mwh_capacity
from {{ref('stg_capacity_contracts')}} c join {{ref('dim_date')}} d on c.service_start<d.month_end and c.service_end>d.month
left join {{ref('int_capacity_history')}} h using(_source,capacity_id)
group by c._source,c.capacity_id,d.month,c.provider,c.region,c.purpose,c.mw,c.service_start,c.service_end,d.month_end
