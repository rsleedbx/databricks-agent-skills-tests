# Add rows from Databricks through a Unity Catalog connection

A notebook or job, serverless included, can append rows to a source table through a Unity Catalog connection with the Spark data source and `mode("append")`, with no database client and no password in the code. Use it for a handful of rows (a CDC or query-based test's inserts); for volume, fill inside the database (SKILL.md).

## Only a TYPE JDBC connection takes writes

A Lakehouse Federation connection (type `POSTGRESQL`, `MYSQL`, `SQLSERVER`, ...) refuses Spark reads and writes alike, whatever the format (`jdbc`, `postgresql`, `sqlserver`, `mysql`) and save mode:

```
[CONNECTION_TYPE_NOT_SUPPORTED_FOR_OPTIONS_INJECTION] Connection with name <name> and type POSTGRESQL is not supported in dataframe options.
```

Writes need a connection of type `JDBC` ([JDBC connection](https://docs.databricks.com/aws/en/connect/jdbc-connection); query federation doesn't write). It names the driver JAR in a Unity Catalog volume, the JDBC URL, the login, and the options a query may set:

1. **The driver JAR** in a volume the users can read (`GRANT READ VOLUME ... TO <group>`): `org.postgresql:postgresql`, `com.mysql:mysql-connector-j`, `com.microsoft.sqlserver:mssql-jdbc` from Maven (check the `.sha1` next to each JAR), copied with `databricks fs cp <jar> dbfs:/Volumes/<catalog>/<schema>/<volume>/`.
2. **A login that may insert into the target table only.** The Lakeflow Connect user only reads; grant it `SELECT, INSERT` on the one table (Postgres: also `USAGE` on the key's sequence), or use a separate writer login.
3. **The connection**, from a request body in a private file (**lfc-source-setup** "Passwords"), then delete the file:

   ```json
   {"name": "<connection>", "connection_type": "JDBC",
    "environment_settings": {"java_dependencies": ["/Volumes/<catalog>/<schema>/<volume>/<driver>.jar"]},
    "options": {"url": "<url>", "user": "<user>", "password": "<password>", "externalOptionsAllowList": "dbtable,query"}}
   ```

   `databricks connections create --json @conn.json --profile <PROFILE>`. The URL and login can't be changed from a query; a new password means creating the connection again.

URLs, with TLS on and the certificate trusted, not TLS off (**lfc-source-setup** step 2):

| Engine | URL |
|---|---|
| PostgreSQL | `jdbc:postgresql://<host>:<port>/<database>?sslmode=require` (encrypts without checking the certificate; a server with `ssl = off` refuses it: [TLS Off on the Server](../../lfc-source-setup/references/engine-postgres.md#tls-off-on-the-server)) |
| MySQL | `jdbc:mysql://<host>:<port>/<database>?sslMode=REQUIRED` |
| SQL Server | `jdbc:sqlserver://<host>:<port>;databaseName=<database>;encrypt=true;trustServerCertificate=true` |

## Append

Write only the columns you set and let the source make the key (`serial`, `AUTO_INCREMENT`, `IDENTITY`): Spark's insert names the DataFrame's columns.

```python
from pyspark.sql import functions as F

rows = spark.range(3).select(F.current_timestamp().alias("dt"), F.concat(F.lit("row "), F.col("id")).alias("note"))
(rows.write.format("jdbc").option("databricks.connection", "<connection>")
     .option("dbtable", "<schema>.<table>").mode("append").save())

n = (spark.read.format("jdbc").option("databricks.connection", "<connection>")
     .option("query", "select count(*) as n from <schema>.<table>").load().first()["n"])
```

`option("query", ...)` is wrapped in `SELECT * FROM (<query>) ...`, so it must be a `SELECT`: MySQL's `SHOW` fails with a syntax error there. That doesn't make the connection read-only: on PostgreSQL and SQL Server a `query` can carry a second statement that writes, so the login's grants are what limit it ([capped-dml-through-jdbc-connection.md](capped-dml-through-jdbc-connection.md#the-allowlist-doesnt-stop-writes-the-logins-grants-do)).

Tested 2026-10-30 on serverless (environment 4), three rows per append, the keys made by the source:

| Engine | Driver | Result | TLS on the session (query through the same connection) |
|---|---|---|---|
| PostgreSQL 16 (container) | 42.7.4 | 0 → 3, then 3 → 6 | `TLSv1.3` after turning `ssl` on (`SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()`) |
| MySQL 8.4 (container) | 9.1.0 | 0 → 3, then 3 → 6 | `TLSv1.3` (`performance_schema.session_status`, `Ssl_version`) |
| SQL Server 2022 (container) | 12.8.1.jre11 | 0 → 3 | not read: `sys.dm_exec_connections` needs `VIEW SERVER STATE`; `encrypt=true` makes the driver refuse an unencrypted session |
