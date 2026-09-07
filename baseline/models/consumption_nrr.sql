with cohort as(select *,case when in_cohort then base_cents else 0 end b,case when in_cohort then current_cents else 0 end c from {{ref('nrr_members')}}),
totals as(select l.lens,l.months,count(p.ultimate_parent) filter(where p.in_cohort)::bigint cohort_customers,
 coalesce(sum(p.base_cents),0)::bigint all_base_cents,coalesce(sum(b),0)::bigint base_revenue_cents,coalesce(sum(c),0)::bigint current_revenue_cents,
 coalesce(sum(greatest(c-b,0)),0)::bigint expansion_cents,
 coalesce(sum(case when c<>0 then greatest(b-c,0) else 0 end),0)::bigint contraction_cents,
 coalesce(sum(case when c=0 then b else 0 end),0)::bigint churn_cents
from {{ref('int_nrr_lenses')}} l left join cohort p using(lens) group by 1,2), labelled as(
select *,case when date '{{var("first_month")}}'>(date '{{var("asof")}}'+1)-interval '12 months'-months*interval '1 month' then 'insufficient_history'
 when base_revenue_cents=0 then 'undefined_zero_base' else 'defined' end status from totals)
select date '{{var("asof")}}' AS month,* exclude(months),
 case when status='defined' then current_revenue_cents::double/base_revenue_cents end nrr,
 case when status='defined' then expansion_cents::double/base_revenue_cents end expansion_share,
 case when status='defined' then contraction_cents::double/base_revenue_cents end contraction_share,
 case when status='defined' then churn_cents::double/base_revenue_cents end churn_share,
 case when status='defined' then base_revenue_cents::double/nullif(all_base_cents,0) end cohort_revenue_coverage
from labelled
