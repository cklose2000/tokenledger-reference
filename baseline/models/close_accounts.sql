select month,account,sum(amount_cents)::bigint amount_cents
from {{ref('journal_lines')}} group by 1,2
