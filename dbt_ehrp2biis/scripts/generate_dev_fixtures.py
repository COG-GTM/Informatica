"""Synthetic EHRP / NKNIGHT source rows for the local DuckDB target (seeds/dev_fixtures).

Column names and types come from the mapping XML (SQ_PS_GVT_JOB ports and lookup ports). Values
are random but deterministic, with edge cases planted on purpose:
  * actions on both sides of a year boundary (event IDs restart from each year's high-water mark)
  * 0 / NULL / non-zero leave balances (zero-to-null), intermittent and full-time schedules
  * award / bonus / severance / buyout / LWOP NOA codes, ERNCD exclusions
  * NULL and all-space strings (Informatica concat and IS_SPACES semantics)
  * missing lookup rows and duplicate lookup rows (Use Any Value)
  * PS_GVT_JOB rows that are not in the trigger table (must be filtered out)
No real person data: names, SSNs and IDs are generated.
"""
import csv
import datetime as dt
import os
import random
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
XML_PATH = os.path.join(PROJECT, "..", "XML", "EHRP2BIIS_UPDATE")
OUT = os.path.join(PROJECT, "seeds", "dev_fixtures")
rng = random.Random(20261016)

KEY = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]
LOOKUPS = {
    "ps_gvt_employment": ("lkp_PS_GVT_EMPLOYMENT", KEY),
    "ps_gvt_pers_nid": ("lkp_PS_GVT_PERS_NID", KEY),
    "ps_gvt_pers_data": ("lkp_PS_GVT_PERS_DATA", KEY),
    "ps_gvt_awd_data": ("lkp_PS_GVT_AWD_DATA", KEY),
    "ps_gvt_citizenship": ("lkp_PS_GVT_CITIZENSHIP", KEY),
    "ps_he_fill_pos": ("lkp_PS_HE_FILL_POS", KEY),
    "ps_gvt_ee_data_trk": ("lkp_PS_GVT_EE_DATA_TRK", KEY + ["GVT_WIP_STATUS"]),
    "ps_jpm_jp_items": ("lkp_PS_JPM_JP_ITEMS", ["JPM_PROFILE_ID", "JPM_CAT_TYPE", "EFF_STATUS", "EFFDT"]),
}

NOA_CODES = ["100", "170", "702", "721", "815", "816", "817", "825", "840", "846", "873", "889",
             "948", "304", "352", "460", "473", "805", "806", "893", "894"]


def duck_type(dtype, precision, scale):
    if dtype in ("string", "varchar2"):
        return "varchar"
    if dtype in ("date/time", "date"):
        return "date"
    return f"decimal({min(int(precision), 38) or 38}, {int(scale)})"


def load_columns():
    root = ET.parse(XML_PATH).getroot()
    trans = {t.get("NAME"): {f.get("NAME"): f.attrib for f in t.findall("TRANSFORMFIELD")}
             for t in root.iter("TRANSFORMATION")}
    conns = list(root.iter("CONNECTOR"))
    sq_used = []
    for c in conns:
        if c.get("FROMINSTANCE") == "SQ_PS_GVT_JOB" and c.get("FROMFIELD") not in sq_used:
            sq_used.append(c.get("FROMFIELD"))
    sq_used = KEY + [c for c in sq_used if c not in KEY]
    job_cols = [(c, duck_type(trans["SQ_PS_GVT_JOB"][c]["DATATYPE"], trans["SQ_PS_GVT_JOB"][c]["PRECISION"],
                              trans["SQ_PS_GVT_JOB"][c]["SCALE"])) for c in sq_used]
    lookup_cols = {}
    for table, (lkp, keys) in LOOKUPS.items():
        used = [c.get("FROMFIELD") for c in conns if c.get("FROMINSTANCE") == lkp]
        cols = keys + [c for c in dict.fromkeys(used) if c not in keys]
        ports = trans[lkp]
        lookup_cols[table] = [(c, duck_type(ports[c]["DATATYPE"], ports[c]["PRECISION"], ports[c]["SCALE"]))
                              for c in cols]
    return job_cols, lookup_cols


def generic(col, typ):
    if typ == "varchar":
        return rng.choice("ABCDEFGH")
    if typ == "date":
        return (dt.date(2015, 1, 1) + dt.timedelta(days=rng.randint(0, 3650))).isoformat()
    scale = int(typ.split(",")[1].rstrip(")"))
    return str(rng.randint(0, 9)) if scale == 0 else f"{rng.randint(0, 9)}.{rng.choice(['00', '25', '50'])}"


def leave_value():
    return rng.choice(["0", "0", "", f"{rng.randint(1, 240)}.{rng.choice(['00', '25', '50', '75'])}"])


def job_row(cols, key, i):
    emplid, empl_rcd, effdt, effseq = key
    row = {c: generic(c, t) for c, t in cols}
    noa = NOA_CODES[i % len(NOA_CODES)]
    sched = ["F", "P", "I", "F", "G"][i % 5]
    row.update({
        "EMPLID": emplid, "EMPL_RCD": str(empl_rcd), "EFFDT": effdt.isoformat(), "EFFSEQ": str(effseq),
        "GVT_NOA_CODE": noa, "HE_NOA_EXT": rng.choice(["0", "1"]),
        "ACTION": "BON" if noa == "825" and i % 2 else rng.choice(["PAY", "PRO", "HIR", "BON"]),
        "ACTION_DT": (effdt - dt.timedelta(days=rng.randint(0, 20))).isoformat(),
        "GVT_WORK_SCHED": sched, "REG_TEMP": rng.choice(["R", "R", "T"]),
        "STD_HOURS": rng.choice(["40.00", "32.00", "20.00", "0.00", ""]),
        "COMPANY": rng.choice(["HE", "HE", ""]), "GVT_SUB_AGENCY": rng.choice(["NI", "FD", "CD", ""]),
        "GVT_XFER_FROM_AGCY": rng.choice(["", " ", "HE15", "AG03"]),
        "GVT_XFER_TO_AGCY": rng.choice(["", "HE38"]),
        "GVT_PAR_AUTH_D1": rng.choice(["reg 315.601", "", "5 usc 3304"]),
        "GVT_PAR_AUTH_D1_2": rng.choice(["  ", " schedule a  ", ""]),
        "GVT_PAR_AUTH_D2": rng.choice(["", "ZLM"]), "GVT_PAR_AUTH_D2_2": rng.choice(["", " opm  "]),
        "GVT_RETIRE_PLAN": rng.choice(["K", "M", "1", "C"]),
        "GVT_TYPE_OF_APPT": rng.choice(["55", "10", "34", "15"]),
        "UNION_CD": rng.choice([" ", "8888", ""]),
        "GVT_RTND_STEP": rng.choice(["0", "3", ""]),
        "GVT_ANNUITY_OFFSET": rng.choice(["0", "", "1250"]),
        "GVT_WIP_STATUS": rng.choice(["PRO", "CAN", "COR"]),
        "GRADE": rng.choice(["09", "11", "13"]), "ACCT_CD": f"CAN{i:04d}",
    })
    for c, t in cols:
        if c.startswith("HE_") and t.startswith("decimal") and c not in ("HE_NOA_EXT",):
            row[c] = leave_value()
    return row


def main():
    job_cols, lookup_cols = load_columns()
    keys = []
    start = dt.date(2025, 12, 1)
    for i in range(36):
        effdt = start + dt.timedelta(days=(i * 3) % 75)          # Dec 2025 .. Feb 2026, with ties
        keys.append((f"E{1000 + i:07d}", 0, effdt, i % 2))
    untriggered = [(f"E{2000 + i:07d}", 0, start + dt.timedelta(days=i), 0) for i in range(6)]

    tables = {"ps_gvt_job": [job_row(job_cols, k, i) for i, k in enumerate(keys + untriggered)]}
    tables["nwk_new_ehrp_actions_tbl"] = [
        {"EMPLID": k[0], "EMPL_RCD": str(k[1]), "EFFDT": k[2].isoformat(), "EFFSEQ": str(k[3])} for k in keys]
    tables["sequence_num_tbl"] = [
        {"EHRP_YEAR": "2025", "EHRP_SEQ_NUMBER": "2025000412"},
        {"EHRP_YEAR": "2026", "EHRP_SEQ_NUMBER": "2026000031"},
    ]

    def key_cols(k):
        return {"EMPLID": k[0], "EMPL_RCD": str(k[1]), "EFFDT": k[2].isoformat(), "EFFSEQ": str(k[3])}

    def lookup_rows(table, keep, extra):
        cols = lookup_cols[table]
        rows = []
        for i, k in enumerate(keys):
            if not keep(i):
                continue
            row = {c: generic(c, t) for c, t in cols}
            row.update(key_cols(k))
            row.update(extra(i, k))
            rows.append(row)
        return rows

    first = ["ALEX", "JORDAN", "SAM", "TAYLOR", "MORGAN", "CASEY"]
    last = ["RIVERA", "CHEN", "PATEL", "NGUYEN", "OKAFOR", "SMITH"]
    tables["ps_gvt_employment"] = lookup_rows("ps_gvt_employment", lambda i: i % 9 != 4, lambda i, k: {
        "BUSINESS_TITLE": rng.choice(["program analyst", "Biologist", "it specialist (infosec)"]),
        "GVT_APPT_LIMIT_HRS": rng.choice(["0", "1040", ""]), "GVT_APPT_LIMIT_DYS": rng.choice(["0", "90"]),
        "GVT_CURR_APT_AUTH1": rng.choice([" ", "ZLM", ""]), "GVT_CURR_APT_AUTH2": rng.choice([" ", "BWA"]),
        "PROBATION_DT": rng.choice(["", "2026-06-30", "2025-03-15"]), "GVT_SPEP": rng.choice(["01", "", "A1"]),
        "GVT_TENURE": rng.choice(["1", "2"]), "GVT_WGI_STATUS": rng.choice(["Y", "N"])})
    tables["ps_gvt_pers_nid"] = lookup_rows("ps_gvt_pers_nid", lambda i: i != 7, lambda i, k: {
        "NATIONAL_ID": f"9{rng.randint(10000000, 99999999)}"})
    tables["ps_gvt_pers_data"] = lookup_rows("ps_gvt_pers_data", lambda i: i != 11, lambda i, k: {
        "FIRST_NAME": rng.choice(first), "LAST_NAME": rng.choice(last), "MIDDLE_NAME": rng.choice(["Q", ""]),
        "CITY": "" if i % 10 == 3 else rng.choice(["BETHESDA", "ATLANTA", "ROCKVILLE"]),
        "STATE": "" if i % 10 == 3 else rng.choice(["MD", "GA"]),
        "ADDRESS1": f"{rng.randint(1, 999)} SYNTHETIC WAY", "POSTAL": f"{rng.randint(20000, 31999)}",
        "SEX": rng.choice(["M", "F"]), "BIRTHDATE": f"19{rng.randint(60, 99)}-0{rng.randint(1, 9)}-15"})
    tables["ps_gvt_awd_data"] = lookup_rows("ps_gvt_awd_data", lambda i: i % 3 != 2, lambda i, k: {
        "ERNCD": rng.choice(["RCR", "RLC", "AWD", ""]), "GOAL_AMT": rng.choice(["2500.00", "500.00", "0"]),
        "GVT_TANG_BEN_AMT": rng.choice(["100.00", ""]), "OTH_HRS": rng.choice(["8.00", "16.00", ""]),
        "OTH_PAY": rng.choice(["1200.00", ""]), "EARNINGS_END_DT": "2027-01-31"})
    tables["ps_gvt_citizenship"] = lookup_rows("ps_gvt_citizenship", lambda i: i != 5, lambda i, k: {
        "CITIZENSHIP_STATUS": rng.choice(["1", "2", "3", "4"])})
    dup = dict(tables["ps_gvt_citizenship"][0]); dup["CITIZENSHIP_STATUS"] = "3"
    tables["ps_gvt_citizenship"].append(dup)                       # multiple match on one key
    tables["ps_he_fill_pos"] = lookup_rows("ps_he_fill_pos", lambda i: i % 4 == 0, lambda i, k: {
        "HE_FILL_POSITION": rng.choice(["N", "R", "T"])})
    tables["ps_gvt_ee_data_trk"] = lookup_rows("ps_gvt_ee_data_trk", lambda i: i % 2 == 0, lambda i, k: {
        "GVT_WIP_STATUS": "PRO", "GVT_DATE_WRK": (k[2] + dt.timedelta(days=2)).isoformat()})
    dup = dict(tables["ps_gvt_ee_data_trk"][0]); dup["GVT_DATE_WRK"] = "2025-12-20"
    tables["ps_gvt_ee_data_trk"].append(dup)
    items = []
    for i, k in enumerate(keys):
        if i % 5 == 1:
            continue
        for cat, status, deg_id, yr in [("DEG", "A", rng.choice(["13", "17", "21"]), rng.choice(["0", "1998", "2011"])),
                                        ("LIC", "A", "99", "2001")] + ([("DEG", "I", "04", "1990")] if i % 6 == 0 else []):
            row = {c: generic(c, t) for c, t in lookup_cols["ps_jpm_jp_items"]}
            row.update({"JPM_PROFILE_ID": k[0], "JPM_CAT_TYPE": cat, "EFF_STATUS": status,
                        "EFFDT": "2020-01-01", "JPM_CAT_ITEM_ID": deg_id, "JPM_INTEGER_2": yr,
                        "MAJOR_CODE": rng.choice(["0401", "1107", "2701"])})
            items.append(row)
    tables["ps_jpm_jp_items"] = items

    types = {"ps_gvt_job": job_cols, **lookup_cols,
             "nwk_new_ehrp_actions_tbl": [("EMPLID", "varchar"), ("EMPL_RCD", "decimal(38, 0)"),
                                          ("EFFDT", "date"), ("EFFSEQ", "decimal(38, 0)")],
             "sequence_num_tbl": [("EHRP_YEAR", "decimal(4, 0)"), ("EHRP_SEQ_NUMBER", "decimal(10, 0)")]}
    yml = ["version: 2", "", "seeds:"]
    for table, rows in tables.items():
        folder = "nknight" if table in ("nwk_new_ehrp_actions_tbl", "sequence_num_tbl") else "ehrp"
        os.makedirs(os.path.join(OUT, folder), exist_ok=True)
        cols = [c for c, _ in types[table]]
        with open(os.path.join(OUT, folder, f"{table}.csv"), "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow([c.lower() for c in cols])
            for r in rows:
                w.writerow([r.get(c, "") for c in cols])
        yml += [f"  - name: {table}", "    config:", "      column_types:"]
        yml += [f"        {c.lower()}: {t}" for c, t in types[table]]
        print(f"{folder}/{table}: {len(rows)} rows, {len(cols)} columns")
    with open(os.path.join(OUT, "_dev_fixtures.yml"), "w") as fh:
        fh.write("\n".join(yml) + "\n")


if __name__ == "__main__":
    main()
