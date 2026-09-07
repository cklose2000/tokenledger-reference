with recursive current_state as (
select * from {{ref('dim_customer_history')}} where valid_to is null
), roots as (
select customer,customer root,parent,[customer] visited from current_state
union all
select r.customer,c.customer,c.parent,list_append(r.visited,c.customer)
from roots r join current_state c on c.customer=r.parent where not list_contains(r.visited,c.customer)
)
select c.customer,c.segment,c.channel,c.country,r.root ultimate_parent,c.ts created_at
from {{ref('stg_customers')}} c join roots r using(customer) where r.parent is null
