{% macro text(field) -%}json_extract_string(feature_json, '$.{{ field }}'){%- endmacro %}
{% macro money(field) -%}cast(cast({{ text(field) }} as decimal(18,2))*100 as bigint){%- endmacro %}
{% macro day(field) -%}cast({{ text(field) }} as date){%- endmacro %}
{% macro integer(field) -%}cast({{ text(field) }} as bigint){%- endmacro %}
{% macro src(kind) -%}{{ source('raw', kind) }}{%- endmacro %}
{% macro cumulative(amount, elapsed, days) -%}
cast((2*cast({{amount}} as hugeint)*({{elapsed}})+({{days}}))//(2*({{days}})) as bigint)
{%- endmacro %}
