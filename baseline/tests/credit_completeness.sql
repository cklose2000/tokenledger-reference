select c.activity_id from {{ref('stg_credits')}} c left join {{ref('int_credit_earned')}} e using(activity_id)
where e.activity_id is null
