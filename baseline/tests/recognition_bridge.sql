with journal as(select month,-sum(amount_cents) revenue from {{ref('journal_lines')}} where account like '4%' group by 1),
earned as(select month,sum(revenue_cents) revenue from {{ref('fact_revenue')}} group by 1)
select coalesce(j.month,e.month) AS month from journal j full join earned e using(month)
where coalesce(j.revenue,0)<>coalesce(e.revenue,0)
