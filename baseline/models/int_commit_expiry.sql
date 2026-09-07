select e.activity_id,e._source,e.customer,e.contract_id,date_trunc('month',e.ts)::date AS month,c.channel,
 greatest(c.commit_cents-coalesce(sum(i.draw_cents),0),0)::bigint remaining_cents,
 list_sort([e.activity_id,c.activity_id]) upstream_ids
from {{ref('stg_expiries')}} e join {{ref('dim_contract')}} c using(_source,contract_id)
left join {{ref('int_invoice_funding')}} i using(_source,contract_id)
group by e.activity_id,e._source,e.customer,e.contract_id,e.ts,c.channel,c.commit_cents,c.activity_id
