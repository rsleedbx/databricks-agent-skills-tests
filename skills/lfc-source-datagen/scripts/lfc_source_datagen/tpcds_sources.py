"""lfc_source_datagen.tpcds_sources: TPC-DS's own steps on lfc_source_datagen.sources.build: two signals a model can
learn in one source. Store returns (tpc/tpcds-returns.sql) rebuilt from the sales, so whether a line comes back follows
its category, its promotion and the household's income; and web ship dates (tpc/tpcds-ship.sql) after the sale by the
ship mode, a slow warehouse and a bulk line. A summary of both.

    from lfc_source_contract.jdbc import dbapi
    from lfc_source_datagen.tpcds_sources import tpcds_sources
    connect = lambda: dbapi(spark, "retail_jdbc", read_connection="retail_pg", database="retail")
    rows = tpcds_sources(connect, {"mass": (1, "40,80,40,15,1,30/5,1,3,2,5,3,80,2,3")}, login="lfc_cdc")
    # per source: store lines, % returned, web lines, % shipped late, median days to ship

<name>:<sf>:<returns>/<ship>, either half empty for none:
  returns: base,fit,promo,income_step,lag_min,lag_span (per mille but the lag days)
  ship:    promise,handle_min,handle_span,mode_step,slow_every,slow_wh,bulk_qty,bulk,jitter (days but slow_every, bulk_qty)
references/tpch-sources.md has the profiles and what they measured. Each signal's parameters are kept in its table's
comment (store_returns, web_sales), so the summary and a report read them from the data.

  lfc-tpcds-sources [--lakebase P [--profile X] | --host H --password-file F ...] [--login R] [--publication NAME]
                    <database> <name>:<sf>[:<returns>/<ship>] ...
"""
from __future__ import annotations

import json
import re
import sys

from lfc_source_contract.jdbc import ident

from .sources import arguments, build, parse, psql_vars, read_sql, run, statements

RETURNS = ("base", "fit", "promo", "income_step", "lag_min", "lag_span")
SHIP = ("promise", "handle_min", "handle_span", "mode_step", "slow_every", "slow_wh", "bulk_qty", "bulk", "jitter")
SIGNALS = {"return_propensity": ("store_returns", "tpcds-returns.sql", RETURNS, ("lag_span",)),   # tpcds.yaml's signals:
           "web_ship_delay": ("web_sales", "tpcds-ship.sql", SHIP, ("handle_span", "slow_every", "jitter"))}


def split(extra: str) -> dict[str, str]:
    """'<returns>/<ship>' -> {signal: its numbers} for the halves given."""
    if not extra:
        return {}
    if extra.count("/") != 1:
        raise ValueError("TPC-DS signals are <returns>/<ship> (either half empty for none)")
    return {s: v for s, v in zip(SIGNALS, extra.split("/")) if v}


def check(srcs) -> None:
    """Each half's form: its count of whole numbers, the spans and the divisors at least 1."""
    for name, _sf, extra in srcs:
        for signal, nums in split(extra).items():
            _table, _sql, names, at_least_1 = SIGNALS[signal]
            if not re.fullmatch(r"[0-9]+(,[0-9]+)*", nums) or nums.count(",") != len(names) - 1:
                raise ValueError(f"{name}: {signal} is {len(names)} whole numbers, comma-separated ({','.join(names)})")
            v = dict(zip(names, map(int, nums.split(","))))
            if min(v[k] for k in at_least_1) < 1:
                raise ValueError(f"{name}: {signal}'s {', '.join(at_least_1)} at least 1")


def signal_comment(signal: str, nums: str) -> str:
    """The signal's table comment: its name and parameters, as JSON."""
    return json.dumps({"signal": signal, "parameters": dict(zip(SIGNALS[signal][2], map(int, nums.split(","))))},
                      separators=(",", ":"))


def steps(name: str, extra: str) -> list[str]:
    """build()'s steps for TPC-DS: each signal the source has (deterministic)."""
    out = []
    for signal, nums in split(extra).items():
        _table, sql, names, _ = SIGNALS[signal]
        out += statements(psql_vars(read_sql(sql), dict(zip(names, nums.split(","))) | {"signal": signal_comment(signal, nums)}))
    return out


def _parameters(cur, s: str, table: str) -> dict | None:
    cur.execute(f"SELECT obj_description('{s}.{table}'::regclass, 'pg_class')")
    comment = (cur.fetchall() or [[None]])[0][0]
    try:
        return json.loads(comment)["parameters"] if comment else None
    except (ValueError, KeyError, TypeError):
        return None


def summary(cur, name: str) -> dict:
    """store lines, % returned (when the source has the returns signal); web lines, % shipped after the promise,
    median days from sale to ship (when it has the ship signal)."""
    s = ident(name)
    out = {"store_lines": None, "returned_pct": None, "web_lines": None, "late_pct": None, "median_ship_days": None}
    if _parameters(cur, s, "store_returns") is not None:
        cur.execute(f"SELECT (SELECT COUNT(*) FROM {s}.store_sales), (SELECT COUNT(*) FROM {s}.store_returns)")
        lines, returned = cur.fetchall()[0]
        out |= {"store_lines": int(lines), "returned_pct": round(100.0 * int(returned) / max(int(lines), 1), 1)}
    ship = _parameters(cur, s, "web_sales")
    if ship is not None:
        cur.execute(f"SELECT COUNT(*), ROUND(100.0 * AVG((d > {int(ship['promise'])})::int), 1), "
                    f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d) FROM ("
                    f"SELECT ws_ship_date_sk - ws_sold_date_sk AS d FROM {s}.web_sales "
                    f"WHERE ws_ship_date_sk IS NOT NULL AND ws_sold_date_sk IS NOT NULL) x")
        lines, late, median = cur.fetchall()[0]
        out |= {"web_lines": int(lines), "late_pct": float(late or 0), "median_ship_days": float(median or 0)}
    return out


def tpcds_sources(connect, sources, login: str | None = None, publication: str | None = None, sessions: int = 4,
                  log=print) -> list[dict]:
    """TPC-DS sources with their signals (the module's docstring); see sources.build."""
    srcs = parse(sources)
    check(srcs)
    return build(connect, "tpcds", srcs, login, publication, sessions, steps, summary, log=log)


def main(argv: list[str] | None = None) -> int:
    x = arguments("lfc-tpcds-sources", "[:<returns>/<ship>]").parse_args(argv)
    return run(x, "tpcds", steps, summary, check)


if __name__ == "__main__":
    sys.exit(main())
