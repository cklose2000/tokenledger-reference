with postings as (
select activity_id,_source,customer,date_trunc('month',ts)::date AS month,
 unnest(['1100','2100']) account,unnest([commit_cents,-commit_cents]) amount_cents,[activity_id] upstream_ids
from {{ref('dim_contract')}}
union all
select activity_id,_source,customer,month,unnest(['2100','1100','4000','4095']),
 unnest([draw_earned_cents,net_earned_cents-draw_earned_cents-fee_earned_cents,-net_earned_cents,fee_earned_cents]),
 list_sort(list_filter([activity_id,contract_activity_id],x->x is not null))
from {{ref('fact_invoice')}}
union all
select 'subscription:'||subscription_id||':'||month::varchar,_source,customer,month,
 unnest(['1100','4010']),unnest([revenue_cents,-revenue_cents]),upstream_ids from {{ref('int_subscription_earned')}}
union all
select activity_id,_source,customer,month,unnest(['2100','4020']),unnest([remaining_cents,-remaining_cents]),upstream_ids
from {{ref('int_commit_expiry')}}
union all
select activity_id,_source,customer,month,unnest(['4090','1100']),unnest([amount_cents,-amount_cents]),upstream_ids
from {{ref('int_credit_earned')}})
select * from postings where amount_cents<>0
