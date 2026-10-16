"""Generate the mart models from the Informatica mapping export.

Reads ../XML/EHRP2BIIS_UPDATE (PowerCenter 9.6.1 export), walks every CONNECTOR
from each target column back to the Source Qualifier / Lookup / Expression port
that feeds it, and writes models/marts/fct_*.sql with one line per target
column (in target-definition order, cast to the target datatype).

Target columns with no inbound connector are emitted as typed NULLs, which is
what PowerCenter writes for unconnected target ports.

Usage:  python scripts/generate_marts.py
"""
import os
import re
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
XML_PATH = os.path.join(PROJECT, "..", "XML", "EHRP2BIIS_UPDATE")

ZERO_TO_NULL = {
    "o_HE_AL_RED_CRED": "he_al_red_cred", "o_HE_AL_BALANCE": "he_al_balance",
    "o_HE_LUMP_HRS": "he_lump_hrs", "o_HE_AL_CARRYOVER": "he_al_carryover",
    "o_HE_RES_LASTYR": "he_res_lastyr", "o_HE_RES_BALANCE": "he_res_balance",
    "o_HE_RES_TWOYRS": "he_res_twoyrs", "o_HE_RES_THREEYRS": "he_res_threeyrs",
    "o_HE_AL_ACCRUAL": "he_al_accrual", "o_HE_AL_TOTAL": "he_al_total",
    "o_HE_AWOP_SEP": "he_awop_sep", "o_HE_AWOP_WIGI": "he_awop_wigi",
    "o_GVT_ANNUITY_OFFSET": "gvt_annuity_offset", "o_HE_SL_RED_CRED": "he_sl_red_cred",
    "o_HE_SL_BALANCE": "he_sl_balance", "o_HE_FROZEN_SL": "he_frozen_sl",
    "o_HE_SL_CARRYOVER": "he_sl_carryover", "o_HE_SL_ACCRUAL": "he_sl_accrual",
    "o_HE_SL_TOTAL": "he_sl_total", "o_APPT_LIMIT_NTE_HRS": "gvt_appt_limit_hrs",
    "YEAR_DEGREE": "jpm_integer_2",
}

CASH_AWARD_NOAS = "('840', '841', '842', '843', '844', '845', '848', '849', '873', '874', '875', '876', '877', '878', '879')"
SEVERANCE_NOAS = "('304', '312', '356', '357')"
RECRUIT = "(gvt_noa_code = '815' or (gvt_noa_code = '948' and he_noa_ext = '0'))"
RELOC = "(gvt_noa_code = '816' or (gvt_noa_code = '948' and he_noa_ext = '1'))"

# exp_MAIN2BIIS output ports -> Snowflake SQL (original Informatica expression in the comment).
EXPRESSIONS = {
    "o_EVENT_ID": "event_id",
    "o_AGCY_ASSIGN_CD": "{{ infa_concat(['company', 'gvt_sub_agency']) }}",
    "o_AGCY_SUBELEMENT_PRIOR_CD": "case when {{ infa_is_spaces_or_null('gvt_xfer_from_agcy') }} then null else {{ infa_concat(['gvt_xfer_from_agcy', \"'00'\"]) }} end",
    "o_BASE_HOURS": "case when gvt_work_sched = 'I' then std_hours else std_hours * 2 end",
    "o_LEG_AUTH_TXT_1": "upper({{ infa_concat(['gvt_par_auth_d1', 'rtrim(gvt_par_auth_d1_2)']) }})",
    "o_LEG_AUTH_TXT_2": "upper({{ infa_concat(['gvt_par_auth_d2', 'rtrim(gvt_par_auth_d2_2)']) }})",
    "o_PERM_TEMP_POSITION_CD": "case when std_hours < 40 then 3 when std_hours >= 40 then 1 else 0 end",
    "o_UNION_CD": "case when {{ infa_is_spaces('union_cd') }} then null else union_cd end",
    "o_EMP_RESID_CITY_STATE_NAME": "{{ infa_concat(['city', \"',  '\", 'state']) }}",
    "o_EFFSEQ": "effseq",
    "o_LOAD_ID": "{{ load_id() }}",
    "o_LOAD_DATE": "{{ load_date() }}",
    "o_LINE_SEQ": "'01N'",
    "o_CASH_AWARD_AMT": f"case when gvt_noa_code in {CASH_AWARD_NOAS} and not (erncd in ('RCR', 'RLC')) then goal_amt when gvt_noa_code in ('817', '889') then goal_amt else null end",
    "o_CASH_AWARD_BNFT_AMT": f"case when gvt_noa_code in {CASH_AWARD_NOAS} and not (erncd in ('RCR', 'RLC')) then gvt_tang_ben_amt else null end",
    "o_RECRUITMENT_BONUS_AMT": f"case when {RECRUIT} then goal_amt else null end",
    "o_RELOCATION_BONUS_AMT": "case when gvt_noa_code in ('816') then goal_amt else null end",
    "o_TIME_OFF_AWARD_AMT": "case when gvt_noa_code in ('846', '847') then oth_hrs else null end",
    "o_TIME_OFF_GRANTED_HRS": "case when gvt_noa_code in ('846', '847') then oth_hrs else null end",
    "o_CITIZENSHIP_STATUS": "case when citizenship_status = '1' or citizenship_status = '2' then '1' else '8' end",
    "o_TSP_VESTING_CD": "case when gvt_retire_plan in ('K', 'M') then case when gvt_type_of_appt in ('55', '34', '36', '46', '44') then 2 else 3 end else 0 end",
    "o_PERMANENT_TEMP_POSITION_CD": "case when reg_temp = 'R' and gvt_work_sched = 'F' then '1' when reg_temp in ('R', 'T') and gvt_work_sched in ('P', 'I') then '3' when reg_temp = 'T' and gvt_work_sched = 'F' then '2' else null end",
    "o_EVENT_SUBMITTED_DTE": "case when gvt_date_wrk is null then action_dt else gvt_date_wrk end",
    "o_RECRUITMENT_EXP_DTE": f"case when {RECRUIT} then earnings_end_dt else null end",
    "o_RELOCATION_EXP_DTE": f"case when {RELOC} then earnings_end_dt else null end",
    "o_GVT_CURR_APT_AUTH1": "case when {{ infa_is_spaces('gvt_curr_apt_auth1') }} then null else gvt_curr_apt_auth1 end",
    "o_GVT_CURR_APT_AUTH2": "case when {{ infa_is_spaces('gvt_curr_apt_auth2') }} then null else gvt_curr_apt_auth2 end",
    "FEGLI_LIVING_BNFT_CD": "case gvt_noa_code when '805' then 'F' when '806' then 'P' else null end",
    "LWOP_START_DATE": "case when gvt_noa_code in ('460', '473') then effdt else null end",
    "BUYOUT_EFFDT": "case when gvt_noa_code = '825' then effdt else null end",
    "o_BUSINESS_TITLE": "upper(business_title)",
    "o_BUYOUT_AMT": "case when gvt_noa_code = '825' and action = 'BON' then goal_amt else null end",
    "o_SEVERANCE_PAY_AMT": f"case when gvt_noa_code in {SEVERANCE_NOAS} then oth_pay else null end",
    "o_SEVERANCE_TOTAL_AMT": f"case when gvt_noa_code in {SEVERANCE_NOAS} then goal_amt else null end",
    "o_SEVERANCE_PAY_START_DTE": f"case when gvt_noa_code in {SEVERANCE_NOAS} then effdt else null end",
    "o_PROBATION_DT": "case when probation_dt is null then null else {{ dbt.dateadd('month', -12, 'probation_dt') }} end",
}
for port, col in ZERO_TO_NULL.items():
    EXPRESSIONS[port] = "{{ zero_to_null('%s') }}" % col

# Post-load corrections from ehrp2biis_afterload.sql applied in the model itself.
# (target, column) -> wrapper taking the already-cast expression.
AFTERLOAD_RULES = {
    # Step 04: UPDATE nwk_action_secondary_tbl SET retnd1_step_cd = NULL
    #          WHERE event_id < 9000000000 AND retnd1_step_cd = '0.0000000000000'
    # ('0.0000000000000' is how PowerCenter renders a numeric 0 into this varchar column.)
    ("NWK_ACTION_SECONDARY_TBL", "RETND1_STEP_CD"):
        lambda expr: f"case when event_id < 9000000000 and gvt_rtnd_step = 0 then null else {expr} end",
}

MODELS = {
    "NWK_ACTION_PRIMARY_TBL": ("fct_action_primary", "event_id"),
    "NWK_ACTION_SECONDARY_TBL": ("fct_action_secondary", "event_id"),
    "EHRP_RECS_TRACKING_TBL": ("fct_tracking", "biis_event_id"),
}


NORM = {"string": "s", "varchar2": "s", "decimal": "n", "number(p,s)": "n", "integer": "n",
        "double": "n", "date/time": "d", "date": "d"}


def target_cast(expr, src_port, tgt):
    """Cast to the target column type the way PowerCenter converts at the target port."""
    typ = sql_type(tgt)
    src_kind, tgt_kind = NORM[src_port["DATATYPE"]], NORM[tgt["DATATYPE"]]
    tgt_len = int(tgt["PRECISION"])
    if tgt_kind == "s" and src_kind == "d":
        # implicit date -> string uses the default MM/DD/YYYY HH24:MI:SS format, truncated to the column
        arg = expr[2:-2].strip() if expr.startswith("{{") else f"'{expr}'"
        return f"cast(left({{{{ infa_date_to_string({arg}) }}}}, {tgt_len}) as {typ})"
    if tgt_kind == "s" and (src_kind == "n" or int(src_port["PRECISION"]) > tgt_len):
        # PowerCenter truncates strings to the target port precision
        return f"cast(left(cast({expr} as varchar), {tgt_len}) as {typ})"
    if tgt_kind == "n" and src_kind == "s":
        # Snowflake TRY_CAST only accepts string input
        return f"try_cast(cast({expr} as varchar) as {typ})"
    return f"cast({expr} as {typ})"


def sql_type(f):
    dt, p, s = f["DATATYPE"], int(f["PRECISION"]), int(f["SCALE"])
    if dt == "varchar2":
        return f"varchar({p})"
    if dt == "date":
        return "date"
    return f"decimal({min(p, 38) if p else 38}, {s})"


def main():
    root = ET.parse(XML_PATH).getroot()
    trans = {t.get("NAME"): {f.get("NAME"): f.attrib for f in t.findall("TRANSFORMFIELD")}
             for t in root.iter("TRANSFORMATION")}
    into = {(c.get("TOINSTANCE"), c.get("TOFIELD")): (c.get("FROMINSTANCE"), c.get("FROMFIELD"))
            for c in root.iter("CONNECTOR")}

    def resolve(inst, field):
        """Follow pass-through ports back to an SQ column, a lookup column or an expression."""
        while True:
            port = trans[inst][field]
            if inst.startswith("exp_") and port["PORTTYPE"] != "INPUT/OUTPUT":
                return EXPRESSIONS[field], f"{inst}.{field}: {' '.join(port['EXPRESSION'].split())}", port
            if inst == "SQ_PS_GVT_JOB" or inst.startswith("lkp_"):
                src = "PS_GVT_JOB" if inst == "SQ_PS_GVT_JOB" else inst
                return field.lower(), f"{src}.{field}", port
            inst, field = into[(inst, field)]

    zero_pairs = []
    write_staging(root)
    for target in root.iter("TARGET"):
        tname = target.get("NAME")
        model, key = MODELS[tname]
        lines, n_mapped = [], 0
        for f in target.findall("TARGETFIELD"):
            col, typ = f.get("NAME"), sql_type(f.attrib)
            if (tname, col) in into:
                expr, lineage, src_port = resolve(*into[(tname, col)])
                n_mapped += 1
                cast = target_cast(expr, src_port, f.attrib)
                if src_port["NAME"] in ZERO_TO_NULL:
                    zero_pairs.append((model, col.lower(), ZERO_TO_NULL[src_port["NAME"]]))
            else:
                lineage, cast = "not connected in mapping", f"cast(null as {typ})"
            if (tname, col) in AFTERLOAD_RULES:
                cast = AFTERLOAD_RULES[(tname, col)](cast)
                lineage += " + afterload step 04"
            lines.append((f"{cast} as {col.lower()}", lineage))
        width = min(max(len(l) for l, _ in lines) + 1, 100)
        body = ",\n".join(f"    {l}" for l, _ in lines)
        body = "\n".join(
            f"{line:<{width + 5}}-- {lin}" for line, (_, lin) in zip(body.split("\n"), lines))
        sql = f"""{{{{ config(materialized='table') }}}}

-- GENERATED by scripts/generate_marts.py from XML/EHRP2BIIS_UPDATE -- do not edit by hand.
-- Informatica target: {tname} ({len(lines)} columns, {n_mapped} connected in m_EHRP2BIIS_UPDATE).

with enriched as (

    select * from {{{{ ref('int_employee_enrichment') }}}}

)

select
{body}
from enriched
"""
        with open(os.path.join(PROJECT, "models", "marts", f"{model}.sql"), "w") as fh:
            fh.write(sql)
        print(f"{model}: {len(lines)} columns ({n_mapped} mapped)")

    pairs = ",\n".join(f"        ('{m}', '{t}', '{s}')" for m, t, s in zero_pairs)
    with open(os.path.join(PROJECT, "macros", "generated_zero_to_null_columns.sql"), "w") as fh:
        fh.write(f"""-- GENERATED by scripts/generate_marts.py -- do not edit by hand.
-- (mart, target column, source column) for every target fed by an IIF(x = 0, NULL, x) port.
{{% macro zero_to_null_columns() %}}
    {{{{ return([
{pairs}
    ]) }}}}
{{% endmacro %}}
""")
    print(f"zero-to-null columns: {len(zero_pairs)}")


def write_staging(root):
    """stg_ps_gvt_job: the SQ_PS_GVT_JOB override query (same column list, same trigger join)."""
    sq = next(t for t in root.iter("TRANSFORMATION") if t.get("NAME") == "SQ_PS_GVT_JOB")
    query = next(a.get("VALUE") for a in sq.findall("TABLEATTRIBUTE") if a.get("NAME") == "Sql Query")
    select_list = re.split(r"\sFROM\s", query.split("SELECT", 1)[1], maxsplit=1)[0]
    cols = [c.strip().split(".", 1)[1].lower() for c in select_list.split(",")]
    body = ",\n".join(f"    job.{c}" for c in cols)
    with open(os.path.join(PROJECT, "models", "staging", "stg_ps_gvt_job.sql"), "w") as fh:
        fh.write(f"""-- GENERATED by scripts/generate_marts.py from the SQ_PS_GVT_JOB SQL override -- do not edit by hand.
-- Reads PS_GVT_JOB restricted to the rows flagged in NWK_NEW_EHRP_ACTIONS_TBL
-- ({len(cols)} columns, inner join on EMPLID / EMPL_RCD / EFFDT / EFFSEQ).

select
{{body}}
from {{{{ source('ehrp', 'ps_gvt_job') }}}} as job
inner join {{{{ ref('stg_nwk_new_ehrp_actions') }}}} as trg
    on  trg.emplid   = job.emplid
    and trg.empl_rcd = job.empl_rcd
    and trg.effdt    = job.effdt
    and trg.effseq   = job.effseq
""".replace("{body}", body))
    print(f"stg_ps_gvt_job: {len(cols)} columns")


if __name__ == "__main__":
    main()
