select _source,activity_id,month from {{ref('journal_lines')}} group by 1,2,3 having sum(amount_cents)<>0
