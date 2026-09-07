with entries as (
select activity_id,_source,customer,month,'commit_drawdown' AS source,'usage' recognition_kind,
 invoice_id source_document,channel,draw_earned_cents revenue_cents,
 list_sort(list_filter([activity_id,contract_activity_id],x->x is not null)) upstream_ids
from {{ref('fact_invoice')}}
union all
select activity_id,_source,customer,month,'usage','usage',invoice_id,channel,
 net_earned_cents-fee_earned_cents-draw_earned_cents,
 list_sort(list_filter([activity_id,contract_activity_id],x->x is not null))
from {{ref('fact_invoice')}}
union all
select 'subscription:'||subscription_id||':'||month::varchar,_source,customer,month,
 'subscription','subscription',subscription_id,'direct',revenue_cents,upstream_ids
from {{ref('int_subscription_earned')}}
union all
select activity_id,_source,customer,month,'commit_drawdown','commit_expiry',contract_id,channel,remaining_cents,upstream_ids
from {{ref('int_commit_expiry')}}
union all
select activity_id,_source,customer,month,'usage','credit',credit_id,channel,-amount_cents,upstream_ids
from {{ref('int_credit_earned')}})
select activity_id||':'||source||':'||recognition_kind||':'||month::varchar activity_id,
 * exclude(activity_id) from entries where revenue_cents<>0
