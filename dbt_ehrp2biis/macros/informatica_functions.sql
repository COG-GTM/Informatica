{#- Snowflake equivalents of the PowerCenter expression semantics used by m_EHRP2BIIS_UPDATE. -#}

{#- Informatica / Oracle `||` ignores NULL operands (and Oracle stores '' as NULL);
    Snowflake `||` returns NULL if any operand is NULL. -#}
{% macro infa_concat(expressions) -%}
    nullif({% for e in expressions %}coalesce(cast({{ e }} as varchar), ''){% if not loop.last %} || {% endif %}{% endfor %}, '')
{%- endmacro %}

{#- IS_SPACES(x): TRUE when x is all spaces; NULL input gives NULL (falls to the IIF else branch). -#}
{% macro infa_is_spaces(column) -%}
    trim({{ column }}) = ''
{%- endmacro %}

{% macro infa_is_spaces_or_null(column) -%}
    ({{ column }} is null or trim({{ column }}) = '')
{%- endmacro %}

{#- IIF(x = 0, NULL, x) -#}
{% macro zero_to_null(column) -%}
    case when {{ column }} = 0 then null else {{ column }} end
{%- endmacro %}

{#- Implicit date -> string conversion at a string port: default format MM/DD/YYYY HH24:MI:SS. -#}
{% macro infa_date_to_string(expression) -%}
    {{ return(adapter.dispatch('infa_date_to_string', 'ehrp2biis')(expression)) }}
{%- endmacro %}

{% macro default__infa_date_to_string(expression) -%}
    to_char({{ expression }}, 'MM/DD/YYYY HH24:MI:SS')
{%- endmacro %}

{% macro duckdb__infa_date_to_string(expression) -%}
    strftime(cast({{ expression }} as timestamp), '%m/%d/%Y %H:%M:%S')
{%- endmacro %}

{#- Run business date, Informatica TRUNC(SYSDATE). -#}
{% macro load_date() -%}
    {%- if var('load_date', none) -%}
        cast('{{ var("load_date") }}' as date)
    {%- else -%}
        current_date
    {%- endif -%}
{%- endmacro %}

{#- o_LOAD_ID = 'NK' || YYYY || MM || DD of the run date. -#}
{% macro load_id() -%}
    'NK' || cast(cast(extract(year from {{ load_date() }}) * 10000
                    + extract(month from {{ load_date() }}) * 100
                    + extract(day from {{ load_date() }}) as integer) as varchar)
{%- endmacro %}
