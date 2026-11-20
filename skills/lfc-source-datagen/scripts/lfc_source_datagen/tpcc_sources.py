"""lfc_source_datagen.tpcc_sources: TPC-C's delivery times a model can learn, as statschema tpcc.yaml's signals: block
says (order_late_delivery, tpc/tpcc-delivery.sql), through lfc_source_datagen.signals; kept for its command and name.

    rows = tpcc_sources(connect, {"local": (1, "3,0,2,5,2,1,2,12,3,2")}, login="lfc_cdc")
    # per source: delivered orders, % late, median days from entry to delivery

  lfc-tpcc-sources [connection options] <database> <name>:<sf>[:<delivery>] ...   (= lfc-sources --model tpcc)
"""
from __future__ import annotations

import sys

from . import signals
from .sources import main as _sources_main

def tpcc_sources(connect, sources, **kw) -> list[dict]:
    """The TPC-C sources with their signals; see signals.model_sources (login, publication, sessions, log)."""
    return signals.model_sources(connect, "tpcc", sources, **kw)


def main(argv: list[str] | None = None) -> int:
    return _sources_main(["--model", "tpcc", "--signals", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    sys.exit(main())
