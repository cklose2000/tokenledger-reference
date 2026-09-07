select m.*,round(m.mwh*c.rate_cents)::bigint cost_cents
from {{ref('stg_compute_meters')}} m join {{ref('stg_capacity_contracts')}} c
using(_source,capacity_id,provider,region,purpose)
where c.ts<=m.ts and m.ts::date>=c.service_start and m.ts::date<c.service_end
