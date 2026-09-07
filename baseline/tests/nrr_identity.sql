select lens from {{ref('consumption_nrr')}} where current_revenue_cents<>base_revenue_cents+expansion_cents-contraction_cents-churn_cents
