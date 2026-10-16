-- All connected lookups of m_EHRP2BIIS_UPDATE as LEFT JOINs on the action key.
-- Every lookup is configured "Lookup policy on multiple match = Use Any Value"; each CTE keeps
-- exactly one row per lookup key (deterministic ordering) so the joins can never fan out.

{%- set action_key = ['emplid', 'empl_rcd', 'effdt', 'effseq'] %}

with actions as (

    select * from {{ ref('int_sequence_generation') }}

),

employment as (
    {{ dedupe_lookup(source('ehrp', 'ps_gvt_employment'), action_key, [
        'business_title', 'cmpny_seniority_dt', 'gvt_appt_expir_dt', 'gvt_appt_limit_dys',
        'gvt_appt_limit_hrs', 'gvt_cnv_begin_date', 'gvt_comp_lvl_perm', 'gvt_curr_apt_auth1',
        'gvt_curr_apt_auth2', 'gvt_detail_expires', 'gvt_rtnd_grade_beg', 'gvt_rtnd_grade_exp',
        'gvt_sabbatic_expir', 'gvt_scd_leo', 'gvt_scd_retire', 'gvt_scd_tsp', 'gvt_spep',
        'gvt_supv_prob_dt', 'gvt_temp_pro_expir', 'gvt_temp_psn_expir', 'gvt_tenure',
        'gvt_wgi_due_date', 'gvt_wgi_status', 'hire_dt', 'last_increase_dt', 'probation_dt',
        'service_dt']) }}
),

pers_nid as (
    {{ dedupe_lookup(source('ehrp', 'ps_gvt_pers_nid'), action_key, ['national_id']) }}
),

pers_data as (
    {{ dedupe_lookup(source('ehrp', 'ps_gvt_pers_data'), action_key, [
        'last_name', 'first_name', 'middle_name', 'address1', 'city', 'state', 'postal',
        'geo_code', 'sex', 'birthdate', 'military_status', 'gvt_cred_mil_svce',
        'gvt_military_comp', 'gvt_mil_resrve_cat', 'gvt_vet_pref_appt', 'ethnic_group',
        'gvt_disability_cd']) }}
),

awards as (
    {{ dedupe_lookup(source('ehrp', 'ps_gvt_awd_data'), action_key, [
        'earnings_end_dt', 'erncd', 'goal_amt', 'gvt_tang_ben_amt', 'oth_hrs', 'oth_pay']) }}
),

citizenship as (
    {{ dedupe_lookup(source('ehrp', 'ps_gvt_citizenship'), action_key, ['citizenship_status']) }}
),

fill_position as (
    {{ dedupe_lookup(source('ehrp', 'ps_he_fill_pos'), action_key, ['he_fill_position']) }}
),

-- lkp_PS_EE_DATA_TRK SQL override: SELECT DISTINCT GVT_DATE_WRK, keys, GVT_WIP_STATUS FROM PS_GVT_EE_DATA_TRK
ee_data_trk as (
    {{ dedupe_lookup(source('ehrp', 'ps_gvt_ee_data_trk'), action_key, ['gvt_date_wrk']) }}
),

-- lkp_PS_JPM_JP_ITEMS SQL override: WHERE jpm_cat_type = 'DEG' AND eff_status = 'A'; condition JPM_PROFILE_ID = EMPLID
degrees as (
    {{ dedupe_lookup(source('ehrp', 'ps_jpm_jp_items'), ['jpm_profile_id'],
        ['jpm_cat_item_id', 'jpm_integer_2', 'major_code'],
        where="jpm_cat_type = 'DEG' and eff_status = 'A'") }}
)

select
    actions.*,

    employment.business_title,
    employment.cmpny_seniority_dt,
    employment.gvt_appt_expir_dt,
    employment.gvt_appt_limit_dys,
    employment.gvt_appt_limit_hrs,
    employment.gvt_cnv_begin_date,
    employment.gvt_comp_lvl_perm,
    employment.gvt_curr_apt_auth1,
    employment.gvt_curr_apt_auth2,
    employment.gvt_detail_expires,
    employment.gvt_rtnd_grade_beg,
    employment.gvt_rtnd_grade_exp,
    employment.gvt_sabbatic_expir,
    employment.gvt_scd_leo,
    employment.gvt_scd_retire,
    employment.gvt_scd_tsp,
    employment.gvt_spep,
    employment.gvt_supv_prob_dt,
    employment.gvt_temp_pro_expir,
    employment.gvt_temp_psn_expir,
    employment.gvt_tenure,
    employment.gvt_wgi_due_date,
    employment.gvt_wgi_status,
    employment.hire_dt,
    employment.last_increase_dt,
    employment.probation_dt,
    employment.service_dt,

    pers_nid.national_id,

    pers_data.last_name,
    pers_data.first_name,
    pers_data.middle_name,
    pers_data.address1,
    pers_data.city,
    pers_data.state,
    pers_data.postal,
    pers_data.geo_code,
    pers_data.sex,
    pers_data.birthdate,
    pers_data.military_status,
    pers_data.gvt_cred_mil_svce,
    pers_data.gvt_military_comp,
    pers_data.gvt_mil_resrve_cat,
    pers_data.gvt_vet_pref_appt,
    pers_data.ethnic_group,
    pers_data.gvt_disability_cd,

    awards.earnings_end_dt,
    awards.erncd,
    awards.goal_amt,
    awards.gvt_tang_ben_amt,
    awards.oth_hrs,
    awards.oth_pay,

    citizenship.citizenship_status,

    fill_position.he_fill_position,

    ee_data_trk.gvt_date_wrk,

    degrees.jpm_cat_item_id,
    degrees.jpm_integer_2,
    degrees.major_code

from actions
left join employment
    on {{ join_on_key('employment', 'actions', action_key) }}
left join pers_nid
    on {{ join_on_key('pers_nid', 'actions', action_key) }}
left join pers_data
    on {{ join_on_key('pers_data', 'actions', action_key) }}
left join awards
    on {{ join_on_key('awards', 'actions', action_key) }}
left join citizenship
    on {{ join_on_key('citizenship', 'actions', action_key) }}
left join fill_position
    on {{ join_on_key('fill_position', 'actions', action_key) }}
left join ee_data_trk
    on {{ join_on_key('ee_data_trk', 'actions', action_key) }}
left join degrees
    on degrees.jpm_profile_id = actions.emplid
