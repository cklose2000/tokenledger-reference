select activity_id,_source,customer,ts,{{text('segment')}} segment,{{text('channel')}} channel,
 {{text('country')}} country,{{text('parent_customer')}} parent
from {{src('customer_created')}}
