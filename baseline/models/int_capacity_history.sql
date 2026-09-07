select _source,{{text('capacity_id')}} capacity_id,ts valid_from,
 lead(ts,1,(date '{{var("asof")}}'+1)::timestamptz) over(partition by _source,{{text('capacity_id')}} order by ts,activity_id) valid_to,
 cast({{text('mw')}} as decimal(24,9)) mw
from {{src('capacity_energized')}}
