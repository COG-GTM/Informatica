{#- One row per lookup key ("Use Any Value"), chosen deterministically. -#}
{% macro dedupe_lookup(relation, key_columns, value_columns, where=none) -%}
    select
        {{ (key_columns + value_columns) | join(',\n        ') }}
    from {{ relation }}
    {%- if where %}
    where {{ where }}
    {%- endif %}
    qualify row_number() over (
        partition by {{ key_columns | join(', ') }}
        order by {% for c in value_columns %}{{ c }} desc nulls last{% if not loop.last %}, {% endif %}{% endfor %}
    ) = 1
{%- endmacro %}

{% macro join_on_key(left_alias, right_alias, key_columns) -%}
    {% for c in key_columns %}{{ left_alias }}.{{ c }} = {{ right_alias }}.{{ c }}{% if not loop.last %} and {% endif %}{% endfor %}
{%- endmacro %}
