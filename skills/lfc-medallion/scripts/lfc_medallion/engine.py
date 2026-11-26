"""engine: lfc_medallion's SQL on a SQL warehouse (lfc_change_source.engine.Warehouse, with sql().collect() as on
Spark), plus what the fan-in planner reads of a warehouse: its units. A warehouse plans many sources' change feeds
faster than a job's session, so the fan-in planner (paths.py) may send a table's MERGE there.

    from lfc_medallion.engine import Warehouse
    wh = Warehouse("abc123")
    wh.sql("SELECT 1 AS one").collect()[0].one
"""
from __future__ import annotations

from lfc_change_source.engine import FLOATS, INTS, NESTED, Row, _Result, row_type, value  # noqa: F401
from lfc_change_source.engine import Warehouse as _Warehouse


class Warehouse(_Warehouse):
    def units(self) -> float:
        """Its capacity for paths.py: the clusters running now (stopped: its minimum), a size up 1.25 times
        (paths.units). Not its maximum: autoscaling adds a cluster only after queries have queued a while, too late
        for a burst of MERGEs that lasts a minute."""
        from .paths import units
        w = self.w.warehouses.get(self.id)
        return units(w.cluster_size, max(w.num_clusters or 0, w.min_num_clusters or 1))

