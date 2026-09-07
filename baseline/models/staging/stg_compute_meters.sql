select activity_id,_source,ts,{{text('capacity_id')}} capacity_id,{{text('provider')}} provider,
 {{text('region')}} region,{{text('purpose')}} purpose,{{text('model')}} model,
 date_trunc('month',{{day('period_start')}})::date AS month,cast({{text('mwh')}} as decimal(28,9)) mwh
from {{src('capacity_consumed')}}
