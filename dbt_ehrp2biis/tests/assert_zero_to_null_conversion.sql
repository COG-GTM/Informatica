-- Every IIF(x = 0, NULL, x) target column: NULL when the source value is 0, otherwise the source
-- value cast to the target type. Columns come from the mapping (macros/generated_zero_to_null_columns.sql).

{%- for mart, target_column, source_column in zero_to_null_columns() %}
{% if not loop.first %}union all{% endif %}
select
    '{{ mart }}.{{ target_column }}' as failing_column,
    m.event_id,
    cast(e.{{ source_column }} as varchar) as source_value,
    cast(m.{{ target_column }} as varchar) as target_value
from {{ ref(mart) }} as m
inner join {{ ref('int_employee_enrichment') }} as e
    on e.event_id = m.event_id
where (e.{{ source_column }} = 0 and m.{{ target_column }} is not null)
   or (e.{{ source_column }} <> 0 and m.{{ target_column }} is null)
{%- endfor %}
