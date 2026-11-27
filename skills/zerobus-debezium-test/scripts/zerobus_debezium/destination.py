"""destination: what Debezium Server's Zerobus sink writes into, in a workspace: the Zerobus endpoint, the bronze change
log (cdc_log), a service principal with an OAuth secret kept only in a secret scope, and its grants. `api` is an
lfc_rest.Rest (REST paths and JSON; the SDK only signs in); SQL runs on lfc_change_source's Warehouse, and the change
log's DDL and grant are lfc_change_source.producer.zerobus_log_sql's.

    api = Rest.connect("<profile>")
    wh = Warehouse.pick(api)
    zb = destination.endpoint(api)
    log = destination.change_log(wh, "main", "cdc", principal=sp["application_id"])
    sp = destination.service_principal(api, "dbz-zerobus", scope="dbz", key="zb-client-secret")
"""
from __future__ import annotations


def q(*parts: str) -> str:
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def endpoint(api) -> str:
    """The workspace's Zerobus endpoint: https://<workspace id>.zerobus.<region>.<cloud domain>. (No other module
    reads the metastore's region and cloud for an endpoint.)"""
    ws = api.get("/api/2.1/unity-catalog/current-metastore-assignment")["workspace_id"]
    ms = api.get("/api/2.1/unity-catalog/metastore_summary")
    domain = {"azure": "azuredatabricks.net", "aws": "cloud.databricks.com", "gcp": "gcp.databricks.com"}[ms["cloud"]]
    return f"https://{ws}.zerobus.{ms['region']}.{domain}"


def change_log(wh, catalog: str, schema: str, principal: str, table: str = "cdc_log") -> str:
    """The schema and the change log (if missing), and the grants the producer's service principal needs, directly on
    the table and its parents (a token scoped to its tables may not see inherited grants). The log's quoted name."""
    from lfc_change_source.producer import (
        zerobus_log_sql,  # here: a notebook imports endpoint() without it
    )
    fq = q(catalog, schema, table)
    wh.sql(f"CREATE SCHEMA IF NOT EXISTS {q(catalog, schema)}")
    wh.sql(f"GRANT USE CATALOG ON CATALOG {q(catalog)} TO `{principal}`")
    wh.sql(f"GRANT USE SCHEMA ON SCHEMA {q(catalog, schema)} TO `{principal}`")
    for s in zerobus_log_sql(fq, principal):
        wh.sql(s)
    return fq


def put_secret(api, scope: str, key: str, value: str) -> str:
    """value into scope/key, the scope made if missing; returns "scope/key", never the value."""
    try:
        api.post("/api/2.0/secrets/scopes/create", {"scope": scope})
    except Exception as e:  # noqa: BLE001 -- the scope is there already (checked below)
        if "ALREADY_EXISTS" not in str(e).upper().replace(" ", "_"):
            raise
    api.post("/api/2.0/secrets/put", {"scope": scope, "key": key, "string_value": value})
    return f"{scope}/{key}"


def service_principal(api, name: str, scope: str, key: str) -> dict:
    """A new service principal and an OAuth secret, the secret put straight into scope/key and never returned.
    {"id", "application_id", "name", "secret": "scope/key"}. (lfc-workspace-setup's check.py only looks principals
    up; none creates one in Python.)"""
    sp = api.post("/api/2.0/preview/scim/v2/ServicePrincipals",
                  {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"], "displayName": name,
                   "active": True})
    sec = api.post(f"/api/2.0/accounts/servicePrincipals/{sp['id']}/credentials/secrets", {})
    ref = put_secret(api, scope, key, sec["secret"])
    return {"id": sp["id"], "application_id": sp["applicationId"], "name": name, "secret": ref}
