select e.activity_id from {{ref('stg_expiries')}} e left join {{ref('dim_contract')}} c using(_source,contract_id)
where c.activity_id is null
union all
select e.activity_id from {{ref('stg_expiries')}} e join {{ref('stg_invoices')}} i using(_source,contract_id)
where i.service_end>e.ts::date
