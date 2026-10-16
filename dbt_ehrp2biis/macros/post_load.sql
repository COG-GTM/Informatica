{#-
    Afterload side effects on the source schemas, off by default (vars.apply_post_load_side_effects).
    They only run after a `dbt run` / `dbt build` in which every node succeeded.
-#}

{% macro _post_load_enabled(results) -%}
    {%- set ok = execute and var('apply_post_load_side_effects', false) and flags.WHICH in ('run', 'build') -%}
    {%- if ok -%}
        {%- for r in results if r.status in ('error', 'fail', 'runtime error') -%}
            {%- set ok = false -%}
            {%- do log("Skipping afterload side effects: " ~ r.node.name ~ " " ~ r.status, info=true) -%}
            {{ return(false) }}
        {%- endfor -%}
    {%- endif -%}
    {{ return(ok) }}
{%- endmacro %}

{% macro _nknight_table(identifier) -%}
    {%- set db = var('nknight_database', target.database) -%}
    {%- set schema = var('nknight_schema', 'NKNIGHT' if target.type == 'snowflake' else target.schema ~ '_nknight') -%}
    {{ db }}.{{ schema }}.{{ identifier }}
{%- endmacro %}

{#- Afterload: execute update_sequence_number_tbl_p. The procedure body is not in the repository;
    this assumes it stores the highest event ID used per EFFDT year (regular events, < 9000000000). -#}
{% macro advance_event_id_high_water_mark(results) -%}
    {%- if _post_load_enabled(results) and var('event_id_strategy', 'high_water_mark') == 'high_water_mark' -%}
        update {{ _nknight_table('sequence_num_tbl') }}
        set ehrp_seq_number = batch.max_event_id
        from (
            select extract(year from event_eff_dte) as ehrp_year, max(event_id) as max_event_id
            from {{ target.database }}.{{ target.schema }}.fct_action_primary
            where event_id < 9000000000
            group by 1
        ) as batch
        where {{ _nknight_table('sequence_num_tbl') }}.ehrp_year = batch.ehrp_year
          and batch.max_event_id > {{ _nknight_table('sequence_num_tbl') }}.ehrp_seq_number
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}

{#- Afterload: truncate table nknight.nwk_new_ehrp_actions_tbl (before the next load). -#}
{% macro truncate_trigger_table(results) -%}
    {%- if _post_load_enabled(results) -%}
        {%- if target.type == 'duckdb' -%}
        delete from {{ _nknight_table('nwk_new_ehrp_actions_tbl') }}
        {%- else -%}
        truncate table {{ _nknight_table('nwk_new_ehrp_actions_tbl') }}
        {%- endif -%}
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
