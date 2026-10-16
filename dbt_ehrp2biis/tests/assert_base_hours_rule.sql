-- o_BASE_HOURS = IIF(GVT_WORK_SCHED = 'I', STD_HOURS, STD_HOURS * 2)
select p.event_id, e.gvt_work_sched, e.std_hours, p.base_hrs
from {{ ref('fct_action_primary') }} as p
inner join {{ ref('int_employee_enrichment') }} as e
    on e.event_id = p.event_id
where cast(case when e.gvt_work_sched = 'I' then e.std_hours else e.std_hours * 2 end as decimal(5, 2))
      is distinct from p.base_hrs
