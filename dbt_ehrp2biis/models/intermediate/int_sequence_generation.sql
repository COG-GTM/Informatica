-- Event ID generation (exp_GET_EFFDT_YEAR -> lkp_OLD_SEQUENCE_NUMBER -> exp_MAIN2BIIS.v_EVENT_ID).
--
-- Informatica reads SQ_PS_GVT_JOB ORDER BY EFFDT and, row by row, keeps
--   v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)
-- i.e. each EFFDT year continues from its last used number in NKNIGHT.SEQUENCE_NUM_TBL.
-- Because rows arrive sorted by EFFDT, years are contiguous and this equals
--   EHRP_SEQ_NUMBER + ROW_NUMBER() OVER (PARTITION BY year ORDER BY EFFDT)
-- (EMPLID / EMPL_RCD / EFFSEQ added as a deterministic tie-break).
-- A year missing from SEQUENCE_NUM_TBL yields a NULL event_id, as in Informatica; the not_null test fails the run.

with actions as (

    select
        *,
        extract(year from effdt) as effdt_year
    from {{ ref('stg_ps_gvt_job') }}

),

last_sequence as (

    select
        ehrp_year,
        ehrp_seq_number
    from {{ source('nknight', 'sequence_num_tbl') }}

)

select
    actions.*,
    last_sequence.ehrp_seq_number,
    {{ generate_event_id(
        year_column='actions.effdt_year',
        last_sequence_column='last_sequence.ehrp_seq_number',
        order_by='actions.effdt, actions.emplid, actions.empl_rcd, actions.effseq'
    ) }} as event_id
from actions
left join last_sequence
    on last_sequence.ehrp_year = actions.effdt_year
