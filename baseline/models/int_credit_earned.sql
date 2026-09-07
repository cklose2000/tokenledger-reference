select c.*,i.channel,list_sort([c.activity_id,i.activity_id]) upstream_ids
from {{ref('stg_credits')}} c join {{ref('stg_invoices')}} i using(_source,invoice_id)
