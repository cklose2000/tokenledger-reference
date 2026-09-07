select *,service_start valid_from,service_end valid_to,
 lead(service_start) over(partition by _source,customer order by service_start,activity_id) next_contract_start
from {{ ref('stg_contracts') }}
