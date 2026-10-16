{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- ACTION_SECONDARY_ALL (ehrp2biis_afterload.sql):
--   insert into action_secondary_all select * from nwk_action_secondary_tbl
--   where event_id in (select event_id from nwk_action_primary_tbl where load_date = trunc(sysdate))

select secondary.*
from {{ ref('fct_action_secondary') }} as secondary
where secondary.event_id in (
    select primary_tbl.event_id
    from {{ ref('fct_action_primary') }} as primary_tbl
    where primary_tbl.load_date = {{ load_date() }}
)
