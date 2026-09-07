with outcomes as(select _source,{{text('trial_id')}} trial_id,max((activity='trial_converted')::int) converted,1 resolved
from (select * from {{src('trial_converted')}} union all select * from {{src('trial_expired')}}) group by 1,2)
select date_trunc('month',t.ts)::date AS month,{{text('plan')}} plan,count(*)::bigint trials_started,
 coalesce(sum(o.converted),0)::bigint trial_cohort_converted,coalesce(sum(o.resolved),0)::bigint trial_cohort_resolved
from {{src('trial_started')}} t left join outcomes o on t._source=o._source and {{text('trial_id')}}=o.trial_id group by 1,2
