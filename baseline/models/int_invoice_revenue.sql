select _source,source_document invoice_id,month,sum(revenue_cents)::bigint revenue_cents
from {{ref('fact_revenue')}} where recognition_kind='usage' group by 1,2,3
