{#- Zero-to-null columns (IIF(x = 0, NULL, x)) must never contain 0. -#}
{% test no_zero_values(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} = 0
{% endtest %}
