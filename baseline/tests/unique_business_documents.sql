select _source,invoice_id AS document from {{ref('stg_invoices')}} group by 1,2 having count(*)<>1
union all
select _source,contract_id from {{ref('dim_contract')}} group by 1,2 having count(*)<>1
union all
select _source,credit_id from {{ref('stg_credits')}} group by 1,2 having count(*)<>1
union all
select _source,contract_id from {{ref('stg_expiries')}} group by 1,2 having count(*)<>1
