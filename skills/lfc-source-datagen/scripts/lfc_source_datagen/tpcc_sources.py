"""lfc_source_datagen.tpcc_sources: TPC-C's own step on lfc_source_datagen.sources.build: delivery times a model can
learn (tpc/tpcc-delivery.sql), so an order's lateness follows a slow district, the carrier's tier, a credit hold and a
backorder instead of the spec's same-day delivery; and a summary of the lateness.

    from lfc_source_contract.jdbc import dbapi
    from lfc_source_datagen.tpcc_sources import tpcc_sources
    connect = lambda: dbapi(spark, "orders_jdbc", read_connection="orders_pg", database="orders")
    rows = tpcc_sources(connect, {"local": (1, "3,0,2,5,2,1,2,15,3,2")}, login="lfc_cdc")
    # per source: delivered orders, % late, median days from entry to delivery

Delivery: promise,handle_min,handle_span,slow_every,slow_district,carrier_step,credit_hold,low_stock,backorder,jitter
(days but slow_every and low_stock); references/tpch-sources.md has the profiles and what they measured. The
parameters are kept in order_line's comment, so the summary and a report read the promise from the data.

  lfc-tpcc-sources [--lakebase P [--profile X] | --host H --password-file F ...] [--login R] [--publication NAME]
                   <database> <name>:<sf>[:<delivery>] ...
"""
from __future__ import annotations

import json
import re
import sys

from lfc_source_contract.jdbc import ident

from .sources import arguments, build, parse, psql_vars, read_sql, run, statements

DELIVERY = ("promise", "handle_min", "handle_span", "slow_every", "slow_district", "carrier_step", "credit_hold",
            "low_stock", "backorder", "jitter")
SIGNAL = "order_late_delivery"   # schemas/tpcc.yaml's signals: name


def check(srcs) -> None:
    """The delivery parameters' form: ten whole numbers, handle_span, slow_every and jitter at least 1."""
    for name, _sf, dl in srcs:
        if dl:
            if not re.fullmatch(r"[0-9]+(,[0-9]+){9}", dl):
                raise ValueError(f"{name}: delivery is 10 whole numbers, comma-separated ({','.join(DELIVERY)})")
            v = dict(zip(DELIVERY, map(int, dl.split(","))))
            if min(v["handle_span"], v["slow_every"], v["jitter"]) < 1:
                raise ValueError(f"{name}: handle_span, slow_every and jitter at least 1")


def signal_comment(dl: str) -> str:
    """order_line's comment: the signal and its parameters, as JSON."""
    return json.dumps({"signal": SIGNAL, "parameters": dict(zip(DELIVERY, map(int, dl.split(","))))},
                      separators=(",", ":"))


def steps(name: str, dl: str) -> list[str]:
    """build()'s step for TPC-C: the delivery times when the source has them (deterministic)."""
    if not dl:
        return []
    values = dict(zip(DELIVERY, dl.split(","))) | {"signal": signal_comment(dl)}
    return statements(psql_vars(read_sql("tpcc-delivery.sql"), values))


def summary(cur, name: str) -> dict:
    """delivered orders, % delivered after the promise, median days from entry to delivery (the promise from
    order_line's comment; none when the source has no delivery times)."""
    s = ident(name)
    cur.execute(f"SELECT obj_description('{s}.order_line'::regclass, 'pg_class')")
    comment = (cur.fetchall() or [[None]])[0][0]
    try:
        promise = json.loads(comment)["parameters"]["promise"] if comment else None
    except (ValueError, KeyError, TypeError):
        promise = None
    if promise is None:
        return {"orders": None, "late_pct": None, "median_days": None}
    cur.execute(f"SELECT COUNT(*), ROUND(100.0 * AVG((d > {int(promise)})::int), 1), "
                f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d) FROM ("
                f"SELECT MAX(ol.ol_delivery_d::date - o.o_entry_d::date) AS d FROM {s}.orders o "
                f"JOIN {s}.order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
                f"WHERE ol.ol_delivery_d IS NOT NULL GROUP BY o.o_w_id, o.o_d_id, o.o_id) x")
    orders, late, median = cur.fetchall()[0]
    return {"orders": int(orders), "late_pct": float(late or 0), "median_days": float(median or 0)}


def tpcc_sources(connect, sources, login: str | None = None, publication: str | None = None, sessions: int = 4,
                 log=print) -> list[dict]:
    """TPC-C sources with their delivery times (the module's docstring); see sources.build."""
    srcs = parse(sources)
    check(srcs)
    return build(connect, "tpcc", srcs, login, publication, sessions, steps, summary, log=log)


def main(argv: list[str] | None = None) -> int:
    x = arguments("lfc-tpcc-sources", "[:<delivery>]").parse_args(argv)
    return run(x, "tpcc", steps, summary, check)


if __name__ == "__main__":
    sys.exit(main())
