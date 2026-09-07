select d.month_end-1 AS month,p.ultimate_parent,sum(p.revenue_cents)::bigint revenue_cents
from {{ref('dim_date')}} d join {{ref('int_parent_revenue')}} p on p.month>=d.month_end-interval '12 months' and p.month<d.month_end
where month(d.month) in(3,6,9,12) and d.month_end-interval '12 months'>=date '{{var("first_month")}}'
group by 1,2
