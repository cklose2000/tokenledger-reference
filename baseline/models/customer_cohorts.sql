select d.month_end-1 AS month,t.threshold_cents,
 count(w.ultimate_parent) filter(where w.revenue_cents>t.threshold_cents)::bigint customers,
 coalesce(sum(w.revenue_cents) filter(where w.revenue_cents>t.threshold_cents),0)::bigint cohort_revenue_cents,
 coalesce(sum(w.revenue_cents),0)::bigint total_revenue_cents,
 coalesce(sum(w.revenue_cents) filter(where w.revenue_cents>t.threshold_cents),0)::double/nullif(sum(w.revenue_cents),0) net_revenue_share
from {{ref('dim_date')}} d cross join (select unnest([10000000,100000000,1000000000])::bigint threshold_cents) t
left join {{ref('int_cohort_windows')}} w on w.month=d.month_end-1
where month(d.month) in(3,6,9,12) and d.month_end-interval '12 months'>=date '{{var("first_month")}}' group by 1,2
