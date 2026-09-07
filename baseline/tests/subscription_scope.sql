select activity_id from (
select * from {{src('subscription_started')}} union all select * from {{src('subscription_renewed')}}
union all select * from {{src('subscription_upgraded')}} union all select * from {{src('subscription_downgraded')}})
where {{text('billing_period')}}<>'monthly' or {{day('period_end')}}<=ts::date
