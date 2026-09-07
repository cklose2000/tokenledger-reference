select activity_id,_source,customer,ts,{{ text('credit_id') }} credit_id,
 {{ text('invoice_id') }} invoice_id,{{ day('period') }} AS month,{{ money('amount_usd') }} amount_cents
from {{ src('credit_issued') }}
