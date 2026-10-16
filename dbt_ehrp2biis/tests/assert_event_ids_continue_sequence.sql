-- Per EFFDT year, event IDs are a gap-free run starting right after SEQUENCE_NUM_TBL.EHRP_SEQ_NUMBER
-- (exp_MAIN2BIIS: v_EVENT_ID = EHRP_SEQ_NUMBER + 1, then + 1 per row).
{{ config(enabled=var('event_id_strategy', 'high_water_mark') == 'high_water_mark') }}

select
    effdt_year,
    min(ehrp_seq_number) as last_sequence,
    min(event_id) as first_event_id,
    max(event_id) as last_event_id,
    count(*) as actions
from {{ ref('int_sequence_generation') }}
group by effdt_year
having min(event_id) <> min(ehrp_seq_number) + 1
    or max(event_id) - min(event_id) + 1 <> count(*)
