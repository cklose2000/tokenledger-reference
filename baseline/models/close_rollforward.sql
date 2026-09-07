with accounts as(select distinct account from {{ref('journal_lines')}}), movements as (
select d.month,a.account,coalesce(c.amount_cents,0)::bigint movement_cents
from {{ref('dim_date')}} d cross join accounts a left join {{ref('close_accounts')}} c using(month,account))
select *,coalesce(sum(movement_cents) over(partition by account order by month rows between unbounded preceding and 1 preceding),0)::bigint beginning_cents,
 sum(movement_cents) over(partition by account order by month)::bigint ending_cents from movements
