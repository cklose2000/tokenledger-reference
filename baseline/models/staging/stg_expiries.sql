select activity_id,_source,customer,ts,{{ text('contract_id') }} contract_id
from {{ src('contract_expired') }}
