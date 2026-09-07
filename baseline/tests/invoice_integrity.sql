select activity_id from {{ref('int_invoice_funding')}}
where gross_cents-discount_cents<>net_cents or fee_cents<0 or fee_cents>net_cents
or service_end<=service_start or (contract_id is not null and contract_activity_id is null)
or (draw_cents>0 and fee_cents>0)
or (contract_id is not null and (service_start<contract_start or service_end>contract_end))
