select activity_id,_source,customer,ts,_stream_position,
 {{ text('contract_id') }} contract_id,{{ text('channel') }} channel,
 {{ day('start') }} service_start,{{ day('end') }} service_end,
 {{ money('commit_usd') }} commit_cents
from (select * from {{ src('contract_signed') }} union all select * from {{ src('contract_renewed') }})
