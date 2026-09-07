select activity_id,_source,customer,ts,activity,{{ text('subscription_id') }} subscription_id,
 {{ text('plan') }} plan,{{ money('price_per_seat') }} price_cents,
 case when activity='subscription_cancelled' then 0
 when activity in ('seat_added','seat_removed') then {{ integer('seats_after') }}
 else {{ integer('seats') }} end seats,
 {{ day('period_end') }} service_end,
 case when activity in ('seat_added','seat_removed') then 20
 when activity in ('subscription_upgraded','subscription_downgraded') then 10
 when activity='subscription_cancelled' then 30 else 0 end precedence
from (
{% for kind in ['subscription_started','subscription_renewed','subscription_upgraded','subscription_downgraded','subscription_cancelled','seat_added','seat_removed'] %}
 select * from {{ src(kind) }} {% if not loop.last %}union all{% endif %}
{% endfor %})
