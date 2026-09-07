select _source,customer,subscription_id,plan,date_trunc('month',ts)::date AS month,
 greatest(seats-previous_seats,0) adds,
 case when activity='subscription_cancelled' then previous_seats else 0 end cancels,
 case when activity<>'subscription_cancelled' then greatest(previous_seats-seats,0) else 0 end removes,
 case when plan<>previous_plan then previous_seats else 0 end transfers,
 0 upgraded,0 downgraded
from {{ref('int_subscription_history')}}
union all
select _source,customer,subscription_id,previous_plan,date_trunc('month',ts)::date,0,0,0,
 case when plan<>previous_plan then -previous_seats else 0 end,
 (activity='subscription_upgraded')::int,(activity='subscription_downgraded')::int
from {{ref('int_subscription_history')}} where plan<>previous_plan or activity in('subscription_upgraded','subscription_downgraded')
