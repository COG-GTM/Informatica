{#-
    Event ID for one EHRP action.

    high_water_mark (default)
        last_sequence + ROW_NUMBER() OVER (PARTITION BY effdt year ORDER BY effdt, ...), where
        last_sequence is the year's row in NKNIGHT.SEQUENCE_NUM_TBL. Same numbering as
        exp_MAIN2BIIS.v_EVENT_ID, and idempotent: re-running the same batch gives the same IDs.
        on-run-end (apply_post_load_side_effects=true) advances SEQUENCE_NUM_TBL afterwards, like
        update_sequence_number_tbl_p in the afterload.

    snowflake_sequence
        NEXTVAL of a Snowflake SEQUENCE created by on-run-start. Globally unique, but IDs no longer
        restart per EFFDT year and change on every rebuild, so it does not reproduce BIIS event IDs.
-#}
{% macro generate_event_id(year_column, last_sequence_column, order_by) -%}
    {%- set strategy = var('event_id_strategy', 'high_water_mark') -%}
    {%- if strategy == 'snowflake_sequence' -%}
        {%- if target.type != 'snowflake' -%}
            {{ exceptions.raise_compiler_error("event_id_strategy=snowflake_sequence requires a Snowflake target") }}
        {%- endif -%}
        {{ event_id_sequence_relation() }}.nextval
    {%- elif strategy == 'high_water_mark' -%}
        {{ last_sequence_column }} + row_number() over (partition by {{ year_column }} order by {{ order_by }})
    {%- else -%}
        {{ exceptions.raise_compiler_error("Unknown event_id_strategy: " ~ strategy) }}
    {%- endif -%}
{%- endmacro %}

{% macro event_id_sequence_relation() -%}
    {{ target.database }}.{{ target.schema }}.{{ var('event_id_sequence_name', 'EHRP_EVENT_ID_SEQ') }}
{%- endmacro %}

{% macro create_event_id_sequence() -%}
    {%- if execute and var('event_id_strategy', 'high_water_mark') == 'snowflake_sequence' and target.type == 'snowflake' -%}
        create schema if not exists {{ target.database }}.{{ target.schema }};
        create sequence if not exists {{ event_id_sequence_relation() }} start = 1 increment = 1 order;
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
