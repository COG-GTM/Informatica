-- Row-count reconciliation: sources -> staging -> intermediate -> marts -> history.
-- Every layer of one run must carry exactly the rows the Informatica Source Qualifier would read
-- (PS_GVT_JOB inner join NWK_NEW_EHRP_ACTIONS_TBL), one row per target per action.
-- Set vars.legacy_target_schema to also compare against the tables loaded by the Informatica session.

with source_qualifier as (

    select count(*) as n
    from {{ source('ehrp', 'ps_gvt_job') }} as job
    inner join {{ source('nknight', 'nwk_new_ehrp_actions_tbl') }} as trg
        on  trg.emplid = job.emplid and trg.empl_rcd = job.empl_rcd
        and trg.effdt = job.effdt and trg.effseq = job.effseq

),

trigger_rows as (

    select count(*) as n from {{ source('nknight', 'nwk_new_ehrp_actions_tbl') }}

),

counts as (

    select 10 as check_order, 'source' as layer, 'EHRP.PS_GVT_JOB' as object_name,
           'all rows (informational)' as check_name,
           count(*) as row_count, cast(null as bigint) as expected_count
    from {{ source('ehrp', 'ps_gvt_job') }}

    union all
    select 20, 'source', 'NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL', 'trigger rows (informational)',
           (select n from trigger_rows), cast(null as bigint)

    union all
    select 30, 'source', 'PS_GVT_JOB x NWK_NEW_EHRP_ACTIONS_TBL', 'SQ_PS_GVT_JOB rows = baseline',
           (select n from source_qualifier), cast(null as bigint)

    union all
    select 40, 'staging', 'stg_nwk_new_ehrp_actions', 'equals trigger rows',
           count(*), (select n from trigger_rows)
    from {{ ref('stg_nwk_new_ehrp_actions') }}

    union all
    select 50, 'staging', 'stg_ps_gvt_job', 'equals SQ rows',
           count(*), (select n from source_qualifier)
    from {{ ref('stg_ps_gvt_job') }}

    union all
    select 60, 'intermediate', 'int_sequence_generation', 'equals SQ rows',
           count(*), (select n from source_qualifier)
    from {{ ref('int_sequence_generation') }}

    union all
    select 70, 'intermediate', 'int_employee_enrichment', 'lookups do not fan out',
           count(*), (select n from source_qualifier)
    from {{ ref('int_employee_enrichment') }}

    union all
    select 80, 'mart', 'fct_action_primary', 'equals SQ rows (NWK_ACTION_PRIMARY_TBL)',
           count(*), (select n from source_qualifier)
    from {{ ref('fct_action_primary') }}

    union all
    select 81, 'mart', 'fct_action_primary', 'distinct event_id',
           count(distinct event_id), (select n from source_qualifier)
    from {{ ref('fct_action_primary') }}

    union all
    select 90, 'mart', 'fct_action_secondary', 'equals SQ rows (NWK_ACTION_SECONDARY_TBL)',
           count(*), (select n from source_qualifier)
    from {{ ref('fct_action_secondary') }}

    union all
    select 100, 'mart', 'fct_tracking', 'equals SQ rows (EHRP_RECS_TRACKING_TBL)',
           count(*), (select n from source_qualifier)
    from {{ ref('fct_tracking') }}

    union all
    select 110, 'history', 'hist_action_primary', 'rows for this load_date (ACTION_PRIMARY_ALL)',
           count(*), (select n from source_qualifier)
    from {{ ref('hist_action_primary') }}
    where load_date = {{ load_date() }}

    union all
    select 120, 'history', 'hist_action_secondary', 'rows for this load (ACTION_SECONDARY_ALL)',
           count(*), (select n from source_qualifier)
    from {{ ref('hist_action_secondary') }}
    where event_id in (select event_id from {{ ref('fct_action_primary') }})

    {%- if var('legacy_target_schema', none) %}
    {%- set legacy_db = var('legacy_target_database', none) or target.database %}
    {%- set legacy = legacy_db ~ '.' ~ var('legacy_target_schema') %}

    union all
    select 200, 'legacy', '{{ legacy }}.NWK_ACTION_PRIMARY_TBL', 'Informatica rows for load_date vs dbt',
           count(*), (select count(*) from {{ ref('fct_action_primary') }})
    from {{ legacy }}.NWK_ACTION_PRIMARY_TBL
    where load_date = {{ load_date() }}

    union all
    select 210, 'legacy', '{{ legacy }}.NWK_ACTION_SECONDARY_TBL', 'Informatica rows for load_date vs dbt',
           count(*), (select count(*) from {{ ref('fct_action_secondary') }})
    from {{ legacy }}.NWK_ACTION_SECONDARY_TBL
    where event_id in (select event_id from {{ legacy }}.NWK_ACTION_PRIMARY_TBL where load_date = {{ load_date() }})

    union all
    select 220, 'legacy', '{{ legacy }}.EHRP_RECS_TRACKING_TBL', 'Informatica rows for load_date vs dbt',
           count(*), (select count(*) from {{ ref('fct_tracking') }})
    from {{ legacy }}.EHRP_RECS_TRACKING_TBL
    where load_date = {{ load_date() }}
    {%- endif %}

)

select
    check_order,
    layer,
    object_name,
    check_name,
    row_count,
    expected_count,
    case
        when expected_count is null then 'INFO'
        when row_count = expected_count then 'PASS'
        else 'FAIL'
    end as status,
    {{ load_date() }} as load_date
from counts
order by check_order
