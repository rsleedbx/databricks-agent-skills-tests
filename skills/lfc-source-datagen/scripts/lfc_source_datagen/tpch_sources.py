"""lfc_source_datagen.tpch_sources: TPC-H's own steps on lfc_source_datagen.sources.build: the spec's words in the text
columns (tpc/tpch-labels.sql) and lead times a model can learn (tpc/tpch-lead-times.sql), so lateness follows the
supplier's nation, the ship mode and the promise instead of TPC-H's random dates; and a summary of the lateness.

    from lfc_source_contract.jdbc import dbapi
    from lfc_source_datagen.tpch_sources import tpch_sources
    connect = lambda: dbapi(spark, "orders_jdbc", read_connection="orders_pg", database="orders")
    rows = tpch_sources(connect, {"express": (0.05, "1,0,1,1,5,0,0,1,2"), "grocery": 0.1}, login="lfc_cdc",
                        labels="anonymous")       # per source: lines, % late, median days from order to receipt

Lead times: commit,handle_min,handle_span,slow_nation,slow_every,air,truck,sea,jitter (days but slow_every);
references/tpch-sources.md has three profiles and what they measured. labels: spec (ALGERIA .. UNITED STATES) or
anonymous (Nation 01 .. 25: with slow nations, real country names read as a claim about real countries).

  lfc-tpch-sources [--lakebase P [--profile X] | --host H --password-file F ...] [--login R] [--labels spec|anonymous]
                   [--publication NAME] <database> <name>:<sf>[:<lead times>] ...
  lfc-tpch-sources [connection options] --slot <slot> [--drop] <database>
"""
from __future__ import annotations

import re
import sys

from lfc_source_contract.jdbc import ident, literal

from .sources import arguments, build, parse, read_sql, run, statements

LEAD = ("commit", "handle_min", "handle_span", "slow_nation", "slow_every", "air", "truck", "sea", "jitter")


def psql_vars(text: str, values: dict) -> str:
    """psql's :'name' (a quoted literal) and :name (as is) for the names given; :: casts stay."""
    text = re.sub(r":'([a-z_]+)'", lambda m: literal(str(values[m.group(1)])), text)
    return re.sub(r"(?<![:\w]):([a-z_]+)\b", lambda m: str(values[m.group(1)]) if m.group(1) in values else m.group(0), text)


def check(srcs) -> None:
    """TPC-H's floor (230 suppliers: scale factor 0.03) and the lead times' form."""
    for name, sf, lt in srcs:
        if sf < 0.03:
            raise ValueError(f"{name}: TPC-H needs scale factor 0.03 or more (230 suppliers); got {sf:g}")
        if lt:
            if not re.fullmatch(r"[0-9]+(,[0-9]+){8}", lt):
                raise ValueError(f"{name}: lead times are 9 whole numbers, comma-separated")
            v = dict(zip(LEAD, map(int, lt.split(","))))
            if min(v["handle_span"], v["slow_every"], v["jitter"]) < 1:
                raise ValueError(f"{name}: handle_span, slow_every and jitter at least 1")


def steps(labels: str = "spec"):
    """build()'s steps for TPC-H: the labels, then the lead times when the source has them (both deterministic)."""
    if labels not in ("spec", "anonymous"):
        raise ValueError("labels: spec or anonymous")

    def run_steps(name: str, lt: str) -> list[str]:
        out = statements(psql_vars(read_sql("tpch-labels.sql"), {"names": labels}))
        if lt:
            out += statements(psql_vars(read_sql("tpch-lead-times.sql"), dict(zip(LEAD, lt.split(",")))))
        return out
    return run_steps


def summary(cur, name: str) -> dict:
    """lines, % received after the promise, median days from order to receipt."""
    cur.execute(f"SELECT COUNT(*), ROUND(100.0 * AVG((l_receiptdate > l_commitdate)::int), 1), "
                f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l_receiptdate - o_orderdate) "
                f"FROM {ident(name)}.lineitem JOIN {ident(name)}.orders ON o_orderkey = l_orderkey")
    lines, late, median = cur.fetchall()[0]
    return {"lines": int(lines), "late_pct": float(late), "median_days": float(median)}


def tpch_sources(connect, sources, login: str | None = None, labels: str = "spec", publication: str | None = None,
                 sessions: int = 4, log=print) -> list[dict]:
    """TPC-H sources with their words and lead times (the module's docstring); see sources.build."""
    srcs = parse(sources)
    check(srcs)
    return build(connect, "tpch", srcs, login, publication, sessions, steps(labels), summary, log=log)


def main(argv: list[str] | None = None) -> int:
    a = arguments("lfc-tpch-sources", "[:<lead times>]")
    a.add_argument("--labels", default="spec", choices=["spec", "anonymous"])
    x = a.parse_args(argv)
    return run(x, "tpch", steps(x.labels), summary, check)


if __name__ == "__main__":
    sys.exit(main())
