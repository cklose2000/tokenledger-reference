select activity_id,_source,ts,{{text('capacity_id')}} capacity_id,{{text('provider')}} provider,
 {{text('region')}} region,{{text('purpose')}} purpose,
 cast({{text('mw')}} as decimal(24,9)) mw,{{money('usd_per_mwh')}} rate_cents,
 {{day('start')}} service_start,{{day('end')}} service_end
from {{src('capacity_contracted')}}
