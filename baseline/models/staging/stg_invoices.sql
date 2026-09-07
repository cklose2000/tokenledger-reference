select activity_id,_source,customer,ts,_stream_position,
 {{ text('invoice_id') }} invoice_id,{{ text('usage_batch_id') }} usage_batch_id,
 {{ text('contract_id') }} contract_id,{{ text('channel') }} channel,
 {{ day('period_start') }} service_start,{{ day('period_end') }} service_end,
 {{ money('gross_usd') }} gross_cents,{{ money('discount_usd') }} discount_cents,
 {{ money('net_usd') }} net_cents,{{ money('channel_fee_usd') }} fee_cents
from {{ src('usage_invoiced') }}
