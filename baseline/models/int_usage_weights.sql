select u.*,m.generation,m.release_date,c.segment,c.ultimate_parent,
 coalesce(r.revenue_cents,0) revenue_cents,coalesce(accounting.fee_earned_cents,0) fee_cents,
 u.tokens::hugeint*a.coefficient_nano energy_weight,
 case when sum(u.rated_cents) over(partition by u._source,u.usage_batch_id,u.month)>0 then u.rated_cents else u.tokens end billing_weight
from {{ref('stg_usage')}} u join {{ref('dim_model')}} m using(model)
join {{ref('dim_customer')}} c using(customer)
join {{source('raw','allocation')}} a using(model,token_type)
left join {{ref('stg_invoices')}} i using(_source,usage_batch_id)
left join {{ref('int_invoice_revenue')}} r using(_source,invoice_id,month)
left join {{ref('fact_invoice')}} accounting using(_source,invoice_id,month)
where u.ts>=m.released_at
