"""lfc_medallion: reusable layers downstream of Lakeflow Connect. discover finds the sources (one bronze schema each)
and their tables; stack builds the n:1 layer, each table of every source in one materialized view with the source;
evidence decides each table's role (fact, dimension, ...) and route from the source's catalog or bronze, then hints;
roles holds the rules (cdc-table-plan's classify.py runs them from files); merge keeps the `merge` route's tables
current from the sources' change feeds, as a job."""
from .discover import sources, tables  # noqa: F401
from .evidence import table_plan  # noqa: F401
from .merge import merge_all, release, routes_to_apply  # noqa: F401
from .stack import source_sql, stack_all, stacked  # noqa: F401
