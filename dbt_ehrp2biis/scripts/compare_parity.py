"""Diff Snowflake parity results (JSON rows saved from the Snowflake run) against the DuckDB marts.

Usage: python scripts/compare_parity.py target_sf/parity/results
Each <mart>.json is the list of row objects returned by Snowflake for target_sf/parity/<mart>.sql.
"""
import datetime as dt
import decimal
import json
import os
import sys

import duckdb

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MARTS = {"fct_action_primary": "event_id", "fct_action_secondary": "event_id", "fct_tracking": "biis_event_id"}


def same(sf_value, duck_value):
    """Compare one cell, interpreting the Snowflake JSON value by the DuckDB column's Python type."""
    if sf_value is None or duck_value is None:
        return sf_value is None and duck_value is None
    if isinstance(duck_value, (dt.datetime, dt.date)):
        return str(sf_value)[:10] == duck_value.isoformat()[:10]
    if isinstance(duck_value, (int, float, decimal.Decimal)):
        return decimal.Decimal(str(sf_value)) == decimal.Decimal(str(duck_value))
    return str(sf_value) == duck_value


def main(results_dir):
    con = duckdb.connect(os.path.join(PROJECT, "target", "ehrp2biis.duckdb"), read_only=True)
    total_bad = 0
    for mart, key in MARTS.items():
        sf_rows = json.load(open(os.path.join(results_dir, f"{mart}.json")))
        rel = con.sql(f"select * from biis.{mart} order by {key}")
        cols = rel.columns
        dk_rows = [dict(zip(cols, r)) for r in rel.fetchall()]
        sf_rows = [{k.lower(): v for k, v in r.items()} for r in sf_rows]
        bad = []
        if len(sf_rows) != len(dk_rows):
            bad.append(f"row count snowflake={len(sf_rows)} duckdb={len(dk_rows)}")
        if sf_rows and set(sf_rows[0]) != set(cols):
            bad.append(f"column sets differ: {set(sf_rows[0]) ^ set(cols)}")
        cells = 0
        for s, d in zip(sf_rows, dk_rows):
            for c in cols:
                cells += 1
                if not same(s.get(c), d[c]):
                    bad.append(f"{key}={d[key]} {c}: snowflake={s.get(c)!r} duckdb={d[c]!r}")
        total_bad += len(bad)
        print(f"{mart}: {len(sf_rows)} rows x {len(cols)} columns = {cells} cells compared, {len(bad)} differences")
        for b in bad[:15]:
            print("   ", b)
    sys.exit(1 if total_bad else 0)


if __name__ == "__main__":
    main(sys.argv[1])
