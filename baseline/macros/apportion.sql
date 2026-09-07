{% macro apportion(relation, grouping, amount, weight) %}
with weights as(select *,sum({{weight}}) over(partition by {{grouping}})::hugeint total_weight from {{relation}}),
parts as(select *,abs({{amount}})::hugeint*{{weight}} product from weights),
floors as(select *,cast(product//nullif(total_weight,0) as bigint) whole,
 row_number() over(partition by {{grouping}} order by product%nullif(total_weight,0) desc,activity_id) ranking from parts)
select activity_id,sign({{amount}})*(whole+case when ranking<=abs({{amount}})-sum(whole) over(partition by {{grouping}}) then 1 else 0 end)::bigint allocated_cents
from floors
{% endmacro %}
