with running as (
 select i.*,coalesce(c.commit_cents,0) commitment,c.activity_id contract_activity_id,
 c.service_start contract_start,c.service_end contract_end,
 coalesce(sum(i.net_cents) over(partition by i._source,i.contract_id
 order by i.service_end,i.activity_id rows between unbounded preceding and 1 preceding),0) prior_usage
 from {{ref('stg_invoices')}} i left join {{ref('dim_contract')}} c using(_source,contract_id)
)
select *,least(net_cents,greatest(commitment-prior_usage,0))::bigint draw_cents
from running
