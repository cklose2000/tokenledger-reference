select *,
{% for name in ['net','fee','draw'] %}
 {{cumulative(name+'_cents','hi_days','service_days')}}-{{cumulative(name+'_cents','lo_days','service_days')}} {{name}}_earned_cents
 {% if not loop.last %},{% endif %}
{% endfor %}
from {{ref('int_invoice_service')}}
