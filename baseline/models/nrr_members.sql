with spans as (
select l.*,date '{{var("asof")}}'+1 end_day,(date '{{var("asof")}}'+1)-interval '12 months' base_end
from {{ref('int_nrr_lenses')}} l), totals as (
select s.lens,s.floor_cents,s.annualization,customers.ultimate_parent,
 sum(case when p.month>=s.base_end-s.months*interval '1 month' and p.month<s.base_end
 then p.consumption_cents+case when s.include_subscription then p.subscription_cents else 0 end else 0 end)::bigint base_cents,
 sum(case when p.month>=s.end_day-s.months*interval '1 month' and p.month<s.end_day
 then p.consumption_cents+case when s.include_subscription then p.subscription_cents else 0 end else 0 end)::bigint current_cents
from spans s cross join (select distinct ultimate_parent from {{ref('dim_customer')}}) customers
left join {{ref('int_parent_revenue')}} p using(ultimate_parent) group by 1,2,3,4)
select lens,ultimate_parent,base_cents,current_cents,base_cents*annualization>=floor_cents in_cohort from totals
