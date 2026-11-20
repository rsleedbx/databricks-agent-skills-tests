"""lfc_source_datagen.tpcds_sources: TPC-DS's two signals a model can learn, store returns and web ship dates, as
statschema tpcds.yaml's signals: block says (tpc/tpcds-returns.sql, tpc/tpcds-ship.sql), through
lfc_source_datagen.signals; kept for its command and name.

    rows = tpcds_sources(connect, {"mass": (1, "40,80,40,15,1,30/6,1,3,2,5,3,80,2,3")}, login="lfc_cdc")
    # per source: store lines, % returned, web lines, % shipped late, median days to ship

  lfc-tpcds-sources [connection options] <database> <name>:<sf>[:<returns>/<ship>] ...   (= lfc-sources --model tpcds)
"""
from __future__ import annotations

import sys

from . import signals
from .sources import main as _sources_main

def tpcds_sources(connect, sources, **kw) -> list[dict]:
    """The TPC-DS sources with their signals; see signals.model_sources (login, publication, sessions, log)."""
    return signals.model_sources(connect, "tpcds", sources, **kw)


def main(argv: list[str] | None = None) -> int:
    return _sources_main(["--model", "tpcds", "--signals", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    sys.exit(main())
