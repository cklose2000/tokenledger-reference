with costs as(select month,model,sum(cost_cents)::bigint model_cost_cents from {{ref('fact_compute_cost')}} where purpose='inference' group by 1,2)
select u.*,coalesce(c.model_cost_cents,0) model_cost_cents from {{ref('int_usage_weights')}} u left join costs c using(month,model)
