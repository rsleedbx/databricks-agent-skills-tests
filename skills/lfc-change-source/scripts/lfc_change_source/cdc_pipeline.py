"""cdc_pipeline: the specs of a Lakeflow Connect database pipeline (REST /api/2.0/pipelines bodies): standard CDC (a
gateway with its own staging schema, then an ingestion pipeline reading it) or integrated CDC (one pipeline,
connector_type CDC). pipeline.upsert_pipeline, or a POST, creates them.

    objs = [table("db", "lfcddemo", "t1", "main", "dest"), table(..., scd="SCD_TYPE_2", auto_full_refresh=True)]
    gw = gateway("me_gw_1", "conn", "main", "dest_stg")
    ap = ingestion("me_ap_1", "main", "dest", objs, gateway_id="<gw id>")      # or connection="conn" (integrated)
"""
from __future__ import annotations

AUTO_FULL_REFRESH = {"enabled": True, "min_interval_hours": 1}


def table(source_catalog: str, source_schema: str, source_table: str, catalog: str, schema: str,
          scd: str = "SCD_TYPE_1", auto_full_refresh: bool = False, **config) -> dict:
    """One ingestion object: a source table into catalog.schema (config: more table_configuration keys)."""
    tc = {"scd_type": scd, **({"auto_full_refresh_policy": AUTO_FULL_REFRESH} if auto_full_refresh else {}), **config}
    return {"table": {"source_catalog": source_catalog, "source_schema": source_schema, "source_table": source_table,
                      "destination_catalog": catalog, "destination_schema": schema, "table_configuration": tc}}


def gateway(name: str, connection: str, catalog: str, staging_schema: str, classic: bool = False,
            configuration: dict | None = None) -> dict:
    """A continuous gateway writing to its own staging schema; serverless, or a classic cluster with the default node
    type (classic=True)."""
    return {"name": name, "catalog": catalog, "schema": staging_schema,
            **({"clusters": [{"label": "default"}]} if classic else {"serverless": True}),
            "continuous": True, "configuration": dict(configuration or {}),
            "gateway_definition": {"connection_name": connection, "gateway_storage_catalog": catalog,
                                   "gateway_storage_schema": staging_schema, "gateway_storage_name": name}}


def ingestion(name: str, catalog: str, schema: str, objects: list, gateway_id: str = "", connection: str = "",
              source_type: str = "", source_configurations: list | None = None, continuous: bool = False,
              configuration: dict | None = None) -> dict:
    """The ingestion pipeline: from the gateway (gateway_id: standard CDC) or the connection itself (integrated CDC).
    source_type and source_configurations: PostgreSQL's slot and publication."""
    if bool(gateway_id) == bool(connection):
        raise ValueError("ingestion: a gateway_id (standard CDC) or a connection (integrated CDC)")
    src = {"ingestion_gateway_id": gateway_id} if gateway_id else {"connection_name": connection, "connector_type": "CDC"}
    if source_type:
        src["source_type"] = source_type
    if source_configurations:
        src["source_configurations"] = list(source_configurations)
    return {"name": name, "catalog": catalog, "schema": schema, "serverless": True,
            **({"continuous": True} if continuous else {}), "configuration": dict(configuration or {}),
            "ingestion_definition": {**src, "objects": list(objects)}}
