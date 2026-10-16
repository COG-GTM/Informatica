-- o_AGCY_ASSIGN_CD = COMPANY || GVT_SUB_AGENCY (NULL operands ignored, as in Informatica/Oracle)
select p.event_id, e.company, e.gvt_sub_agency, p.agcy_assign_cd
from {{ ref('fct_action_primary') }} as p
inner join {{ ref('int_employee_enrichment') }} as e
    on e.event_id = p.event_id
where p.agcy_assign_cd is distinct from
      nullif(coalesce(e.company, '') || coalesce(e.gvt_sub_agency, ''), '')
