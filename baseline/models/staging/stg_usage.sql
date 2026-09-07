select activity_id,_source,customer,ts,date_trunc('month',ts)::date AS month,
 {{text('usage_batch_id')}} usage_batch_id,{{text('model')}} model,{{text('token_type')}} token_type,
 {{text('channel')}} channel,{{text('workload')}} workload,{{integer('tokens')}} tokens,
 greatest(round({{integer('tokens')}}::double*cast({{text('effective_price_per_mtok')}} as double)/10000),0)::bigint rated_cents
from {{src('tokens_processed')}}
