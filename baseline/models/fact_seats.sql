with flows as(select _source,customer,subscription_id,plan,month,sum(adds)::bigint gross_adds,sum(cancels)::bigint cancelled_seats,
sum(removes)::bigint seat_removals,sum(transfers)::bigint plan_transfers_net,max(upgraded) upgraded,max(downgraded) downgraded
from {{ref('int_seat_movements')}} group by 1,2,3,4,5), keys as(
select _source,customer,subscription_id,plan,month from flows union
select _source,customer,subscription_id,plan,month from {{ref('int_seat_exposure')}} union
select _source,customer,subscription_id,plan,month from {{ref('int_seat_balances')}}), joined as(
select k.*,coalesce(b.beginning_seats,0) beginning_seats,coalesce(b.ending_seats,0) ending_seats,
coalesce(e.seat_days,0) seat_days,coalesce(e.account_days,0) account_days,coalesce(e.price_days,0) price_days,
coalesce(f.gross_adds,0) gross_adds,coalesce(f.cancelled_seats,0) cancelled_seats,coalesce(f.seat_removals,0) seat_removals,
coalesce(f.plan_transfers_net,0) plan_transfers_net,coalesce(f.upgraded,0) upgraded,coalesce(f.downgraded,0) downgraded,
coalesce(r.revenue_cents,0) earned,d.days
from keys k join {{ref('dim_date')}} d using(month)
left join {{ref('int_seat_balances')}} b using(_source,customer,subscription_id,plan,month)
left join {{ref('int_seat_exposure')}} e using(_source,customer,subscription_id,plan,month)
left join flows f using(_source,customer,subscription_id,plan,month)
left join {{ref('int_subscription_earned')}} r using(_source,customer,subscription_id,month)), weighted as(
select *,earned::hugeint*price_days product,sum(price_days) over(partition by _source,customer,subscription_id,month) denominator from joined), allocated as(
select *,coalesce(product//nullif(denominator,0),0)::bigint whole,
row_number() over(partition by _source,customer,subscription_id,month order by product%nullif(denominator,0) desc,plan) ranking from weighted)
select * exclude(price_days,earned,product,denominator,whole,ranking),
whole+case when ranking<=earned-sum(whole) over(partition by _source,customer,subscription_id,month) then 1 else 0 end revenue_cents from allocated
