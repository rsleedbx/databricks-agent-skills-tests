"""lfc_source_contract.federation: a Unity Catalog connection of a database's own type (POSTGRESQL, SQLSERVER, MYSQL,
ORACLE), the kind Lakeflow Connect's pipelines, remote_query and Lakehouse Federation read through. TYPE JDBC
connections (writes from Spark) are lfc_source_contract.jdbc's.

    from lfc_source_contract import federation
    body = federation.body("me-mssql-vm", "sqlserver", "203.0.113.10", 30021, "lfc_user", password)
    federation.upsert(api, body)          # api: an lfc_rest.Rest; created, or its options put when it exists

The options per engine are lfc-source-setup's step 2: TLS on with the server's certificate trusted (never off), and
Oracle's service name: the CDB's (FREE on Oracle Free) for a common user like C##LFC_USER, with the PDB as a
pipeline's source_catalog; the user in upper case.
"""
from __future__ import annotations

TYPES = {"postgres": "POSTGRESQL", "sqlserver": "SQLSERVER", "mysql": "MYSQL", "oracle": "ORACLE"}


def body(name: str, engine: str, host: str, port: int, user: str, password: str, service: str | None = None,
         comment: str | None = None) -> dict:
    """The create request. Oracle needs service (the CDB's, for a common user)."""
    if engine not in TYPES:
        raise ValueError(f"federation: {engine}: one of {', '.join(TYPES)} (Db2, Informix: lfc_source_contract.jdbc)")
    opts = {"host": host, "port": str(port), "user": user, "password": password}
    if engine == "sqlserver":
        opts["trustServerCertificate"] = "true"
    if engine == "oracle":
        if not service:
            raise ValueError("federation: oracle needs service (the CDB's service name)")
        opts["service_name"] = service
        opts["user"] = user.upper()
    b = {"name": name, "connection_type": TYPES[engine], "options": opts}
    if comment:
        b["comment"] = comment
    return b


def upsert(api, b: dict) -> str:
    """Create the connection, or update its options (a re-created database has a new password). Its name."""
    try:
        api.post("/api/2.1/unity-catalog/connections", b)
    except Exception as e:  # noqa: BLE001 -- it exists: update it (other errors are raised by the update)
        if "ALREADY_EXISTS" not in str(e).upper().replace(" ", "_"):
            raise
        api.patch(f"/api/2.1/unity-catalog/connections/{b['name']}", {"options": b["options"]})
    return b["name"]
