-- NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL: the actions queued for this run.

select
    emplid,
    empl_rcd,
    effdt,
    effseq
from {{ source('nknight', 'nwk_new_ehrp_actions_tbl') }}
