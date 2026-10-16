{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- ACTION_PRIMARY_ALL (ehrp2biis_afterload.sql):
--   insert into action_primary_all select * from nwk_action_primary_tbl where load_date = trunc(sysdate)
-- delete+insert on event_id also covers the afterload "update cancelled actions" block, which deletes
-- and re-inserts events that are delivered again.

select *
from {{ ref('fct_action_primary') }}
where load_date = {{ load_date() }}
