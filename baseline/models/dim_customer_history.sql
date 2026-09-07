with versions as (
select activity_id,customer,ts,parent from {{ref('stg_customers')}}
union all
select activity_id,customer,ts,{{text('ultimate_parent')}} parent from {{src('customer_merged')}})
select v.*,c.segment,c.channel,c.country,v.ts valid_from,
 lead(v.ts) over(partition by v.customer order by v.ts,v.activity_id) valid_to
from versions v join {{ref('stg_customers')}} c using(customer)
