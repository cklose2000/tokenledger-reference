with grouped as(select month,model,generation,release_date,channel,workload,sum(revenue_cents)::bigint revenue_cents
from {{ref('int_vintage_revenue')}} group by 1,2,3,4,5,6), totals as(
select month,channel,workload,sum(revenue_cents) total,
 sum(case when release_date>=month+interval '1 month'-interval '12 months' then revenue_cents else 0 end) recent
from grouped group by 1,2,3)
select g.month,g.model,g.generation,date_diff('month',g.release_date,g.month) AS months_since_release,g.channel,g.workload,g.revenue_cents,
 g.revenue_cents::double/nullif(t.total,0) revenue_share,t.recent::double/nullif(t.total,0) released_last_12_months_share
from grouped g join totals t using(month,channel,workload)
