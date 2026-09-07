select month,model,generation,release_date,channel,workload,net_revenue_cents revenue_cents from {{ref('fact_usage')}}
union all
select r.month,r.model,m.generation,m.release_date,r.channel,r.workload,r.revenue_cents
from {{ref('int_revenue_attributes')}} r left join {{ref('dim_model')}} m using(model)
where r.recognition_kind<>'usage'
