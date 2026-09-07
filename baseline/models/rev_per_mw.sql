with capacity as(select purpose,sum(contracted_mwh_capacity) contracted,sum(energized_mwh_capacity) energized
from {{ref('fact_compute_capacity')}} where month>=(date '{{var("asof")}}'+1)-interval '12 months' group by 1),
meters as(select purpose,sum(mwh) consumed,sum(cost_cents)::bigint cost_cents from {{ref('fact_compute_cost')}}
where month>=(date '{{var("asof")}}'+1)-interval '12 months' group by 1),
revenue as(select coalesce(sum(revenue_cents),0) amount from {{ref('fact_revenue')}} where month>=(date '{{var("asof")}}'+1)-interval '12 months'),
cal as(select date_diff('hour',(date '{{var("asof")}}'+1)-interval '12 months',date '{{var("asof")}}'+1) AS hours,
 date '{{var("first_month")}}'<=(date '{{var("asof")}}'+1)-interval '12 months' complete)
select date '{{var("asof")}}' AS month,c.purpose,case when complete then 'defined' else 'insufficient_history' end status,
 case when complete and c.purpose='inference' then amount::bigint end t12m_net_revenue_cents,
 coalesce(m.cost_cents,0) cost_cents,c.contracted/hours average_contracted_mw,c.energized/hours average_energized_mw,
 coalesce(m.consumed,0)/hours utilized_mw,coalesce(m.consumed,0)/nullif(c.energized,0) energized_utilization,
 case when complete and c.purpose='inference' then amount*hours/100.0/nullif(c.contracted,0) end revenue_per_contracted_mw,
 case when complete and c.purpose='inference' then amount*hours/100.0/nullif(m.consumed,0) end revenue_per_utilized_mw
from capacity c left join meters m using(purpose) cross join revenue cross join cal
