with batches as (
select _source,usage_batch_id,case when count(distinct model)=1 then min(model) else 'unallocated' end model,
 case when count(distinct workload)=1 then min(workload) else 'unallocated' end workload
from {{ref('stg_usage')}} group by 1,2)
select r.*,coalesce(b.model,'unallocated') model,coalesce(b.workload,'unallocated') workload
from {{ref('fact_revenue')}} r
left join {{ref('stg_credits')}} c on r._source=c._source and r.recognition_kind='credit' and r.source_document=c.credit_id
left join {{ref('stg_invoices')}} i on r._source=i._source and i.invoice_id=
 case when r.recognition_kind='usage' then r.source_document when r.recognition_kind='credit' then c.invoice_id end
left join batches b on b._source=i._source and b.usage_batch_id=i.usage_batch_id
