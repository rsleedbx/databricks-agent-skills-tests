"""lfc_medallion: reusable layers downstream of Lakeflow Connect. discover finds the sources (one bronze schema each)
and their tables; stack builds the n:1 layer, each table of every source in one materialized view with the source."""
from .discover import sources, tables  # noqa: F401
from .stack import stack_all, union_sql  # noqa: F401
