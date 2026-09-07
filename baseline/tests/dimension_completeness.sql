select 'customer_population' failure where (select count(*) from {{ref('dim_customer')}})<>(select count(*) from {{ref('stg_customers')}})
union all select 'token_population' where (select count(*) from {{ref('fact_usage')}})<>(select count(*) from {{ref('stg_usage')}})
