"""Build self-contained Snowflake SELECTs for a cross-engine parity check.

Takes the Snowflake-compiled models (dbt compile --target snowflake --target-path target_sf) and
inlines them as CTEs over the dev fixture rows loaded in the local DuckDB file, so the exact SQL
dbt would run in Snowflake can be executed with read-only access (no CREATE privileges needed).
Writes target_sf/parity/<mart>.sql; run them in Snowflake and diff against the DuckDB marts with
scripts/compare_parity.py.

Usage:
  dbt seed && dbt build --exclude resource_type:seed --vars '{load_date: "2026-02-27"}'
  dbt compile --target snowflake --target-path target_sf --no-populate-cache --vars '{load_date: "2026-02-27"}'
  python scripts/build_snowflake_parity_sql.py
"""
import datetime as dt
import decimal
import os
import re

import duckdb

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPILED = os.path.join(PROJECT, "target_sf", "compiled", "ehrp2biis", "models")
OUT = os.path.join(PROJECT, "target_sf", "parity")
CHAIN = ["staging/stg_nwk_new_ehrp_actions", "staging/stg_ps_gvt_job",
         "intermediate/int_sequence_generation", "intermediate/int_employee_enrichment"]
MARTS = ["marts/fct_action_primary", "marts/fct_action_secondary", "marts/fct_tracking"]


def literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, (dt.date, dt.datetime)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (int, float, decimal.Decimal)):
        return f"'{v}'"
    return "'" + str(v).replace("'", "''") + "'"


def fixture_ctes(con):
    ctes = []
    tables = con.sql("""select table_schema, table_name from information_schema.tables
                        where table_schema in ('biis_ehrp', 'biis_nknight') order by 1, 2""").fetchall()
    for schema, table in tables:
        cols = con.sql(f"""select column_name, data_type from information_schema.columns
                           where table_schema = '{schema}' and table_name = '{table}'
                           order by ordinal_position""").fetchall()
        rows = con.sql(f'select * from {schema}."{table}"').fetchall()
        select = ",\n        ".join(
            f"cast(column{i + 1} as {typ.replace('VARCHAR', 'varchar')}) as {name}"
            for i, (name, typ) in enumerate(cols))
        values = ",\n        ".join("(" + ", ".join(literal(v) for v in r) + ")" for r in rows)
        ctes.append(f"src_{table} as (\n    select\n        {select}\n    from values\n        {values}\n)")
    return ctes


def model_cte(path):
    sql = open(os.path.join(COMPILED, path + ".sql")).read()
    sql = re.sub(r"\bBIIS\.(EHRP|NKNIGHT)\.(\w+)", r"src_\2", sql)
    sql = re.sub(r"\bBIIS\.INFO_TARGET\.(\w+)", r"\1", sql)
    return f"{os.path.basename(path)} as (\n{sql.strip()}\n)"


def main():
    os.makedirs(OUT, exist_ok=True)
    con = duckdb.connect(os.path.join(PROJECT, "target", "ehrp2biis.duckdb"), read_only=True)
    base = fixture_ctes(con) + [model_cte(p) for p in CHAIN]
    for mart in MARTS:
        name = os.path.basename(mart)
        key = "biis_event_id" if name == "fct_tracking" else "event_id"
        sql = "with\n" + ",\n\n".join(base + [model_cte(mart)]) + f"\n\nselect * from {name} order by {key}\n"
        with open(os.path.join(OUT, f"{name}.sql"), "w") as fh:
            fh.write(sql)
        print(f"{name}: {len(sql):,} chars")


if __name__ == "__main__":
    main()
