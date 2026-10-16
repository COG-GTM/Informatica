{#-
    Afterload side effects on the source schemas, off by default (vars.apply_post_load_side_effects).
    They run only after a `dbt run` / `dbt build` that successfully built every pipeline model
    (a partial selection never finalizes a batch), and both statements run in one transaction so
    the high-water mark never advances without the trigger rows being consumed (or vice versa).
-#}


{% macro _post_load_enabled(results) -%}
    {%- if not (execute and var('apply_post_load_side_effects', false) and flags.WHICH in ('run', 'build')) -%}
        {{ return(false) }}
    {%- endif -%}
    {%- set required = ['stg_nwk_new_ehrp_actions', 'stg_ps_gvt_job', 'int_sequence_generation',
                        'int_employee_enrichment', 'fct_action_primary', 'fct_action_secondary',
                        'fct_tracking', 'hist_action_primary', 'hist_action_secondary'] -%}
    {%- set succeeded = [] -%}
    {%- for r in results -%}
        {%- if r.status in ('error', 'fail', 'runtime error', 'skipped') -%}
            {%- do log("Skipping afterload side effects: " ~ r.node.name ~ " " ~ r.status, info=true) -%}
            {{ return(false) }}
        {%- endif -%}
        {%- if r.node.resource_type == 'model' and r.status == 'success' -%}
            {%- do succeeded.append(r.node.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set missing = required | reject('in', succeeded) | list -%}
    {%- if missing -%}
        {%- do log("Skipping afterload side effects: not built in this run: " ~ missing | join(', '), info=true) -%}
        {{ return(false) }}
    {%- endif -%}
    {{ return(true) }}
{%- endmacro %}

{% macro _safe_identifier(value) -%}
    {%- if not modules.re.match('^[A-Za-z_][A-Za-z0-9_$]*$', value | string) -%}
        {{ exceptions.raise_compiler_error("Invalid identifier for afterload side effects: " ~ value) }}
    {%- endif -%}
    {{ value }}
{%- endmacro %}

{% macro _nknight_table(identifier) -%}
    {%- set db = var('nknight_database', target.database) -%}
    {%- set schema = var('nknight_schema', 'NKNIGHT' if target.type == 'snowflake' else target.schema ~ '_nknight') -%}
    {{ _safe_identifier(db) }}.{{ _safe_identifier(schema) }}.{{ _safe_identifier(identifier) }}
{%- endmacro %}

{#-
    Afterload finalization, in one transaction:
      1. execute update_sequence_number_tbl_p - the procedure body is not in the repository; this assumes
         it stores the highest event ID used per EFFDT year (regular events, < 9000000000);
      2. truncate table nknight.nwk_new_ehrp_actions_tbl - done as DELETE so it stays inside the
         transaction (Snowflake TRUNCATE commits implicitly).
-#}
{% macro finalize_batch(results) -%}
    {%- if _post_load_enabled(results) -%}
        {%- set seq = _nknight_table('sequence_num_tbl') -%}
        begin;
        {% if var('event_id_strategy', 'high_water_mark') == 'high_water_mark' -%}
        update {{ seq }}
        set ehrp_seq_number = batch.max_event_id
        from (
            select extract(year from event_eff_dte) as ehrp_year, max(event_id) as max_event_id
            from {{ _safe_identifier(target.database) }}.{{ _safe_identifier(target.schema) }}.fct_action_primary
            where event_id < 9000000000
            group by 1
        ) as batch
        where {{ seq }}.ehrp_year = batch.ehrp_year
          and batch.max_event_id > {{ seq }}.ehrp_seq_number;
        {% endif -%}
        delete from {{ _nknight_table('nwk_new_ehrp_actions_tbl') }};
        commit;
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
