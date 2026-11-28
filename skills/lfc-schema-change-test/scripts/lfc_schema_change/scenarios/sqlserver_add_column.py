"""sqlserver-add-column: when does ADD COLUMN on a SQL Server source need a full refresh? Five tables (change tracking:
intpk, afrpk with the auto full refresh policy; CDC: dtix with no primary key, cdcpk, scd2pk as SCD type 2), then each
kind of added column: nullable (C1), NOT NULL DEFAULT (C2), nullable with a default (C3), identity and computed (C4, C5).
EXCLUDE_DTIX=1 leaves the keyless table out (on integrated CDC its snapshot is empty and its refresh fails the update).
"""
from __future__ import annotations

SHORT = "addcol"
TAB = "\t"

SETUP = """CREATE TABLE lfcddemo.intpk (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), dt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE lfcddemo.dtix (id INT NOT NULL, v VARCHAR(20), dt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE INDEX dtix_dt ON lfcddemo.dtix(dt);
CREATE TABLE lfcddemo.cdcpk (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), dt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE lfcddemo.afrpk (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), dt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE lfcddemo.scd2pk (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), dt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
""" + "".join(f"INSERT INTO lfcddemo.{t} (id, v) SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 'before' "
              "FROM sys.objects;\n" for t in ("intpk", "dtix", "cdcpk", "afrpk", "scd2pk"))


def run(r) -> None:
    db = r.db
    db.create(SETUP)
    db.utility_objects()
    db.track("ct", ["intpk", "afrpk"])
    db.track("cdc", ["dtix", "cdcpk", "scd2pk"])   # SCD_TYPE_2 needs CDC (on change tracking none of its changes apply)
    seen = r.rq("SELECT (SELECT COUNT(*) FROM lfcddemo.intpk) AS intpk_rows, (SELECT COUNT(*) FROM lfcddemo.dtix) AS "
                "dtix_rows, (SELECT COUNT(*) FROM lfcddemo.cdcpk) AS cdcpk_rows, (SELECT COUNT(*) FROM lfcddemo.afrpk) "
                "AS afrpk_rows, dbo.lakeflowUtilityVersion() AS util")
    r.log(f"through the connection: {seen}")
    if seen.split(TAB)[:4] != ["10"] * 4:
        raise r.Setup(f"serverless can't read the source through {r.connection}")

    skip_dtix = r.e.get("EXCLUDE_DTIX") == "1"
    objs = [r.obj("intpk"), *([] if skip_dtix else [r.obj("dtix")]), r.obj("cdcpk"),
            r.obj("afrpk", auto_full_refresh=True), r.obj("scd2pk", scd="SCD_TYPE_2")]
    r.pipelines_create(objs)
    dt = "intpk" if skip_dtix else "dtix"          # stands in for dtix in shared queries
    fp = ["intpk", dt, "cdcpk", "afrpk", "scd2pk"]
    t = r.t

    def upd(refresh=None):
        return r.update(refresh, fp_tables=fp)

    def settle(table, col):
        r.settle(table, col, fp_tables=fp)

    upd()
    if not r.ok_state():
        r.claim("baseline", "FAIL", f"the first update: {r.state} {' '.join(r.errs)}")
        return
    counts = f"SELECT (SELECT COUNT(*) FROM {t}.intpk), (SELECT COUNT(*) FROM {t}.{dt}), (SELECT COUNT(*) FROM {t}.cdcpk), " \
             f"(SELECT COUNT(*) FROM {t}.afrpk)"
    got = r.q_or(counts)
    if got != TAB.join(["10"] * 4):               # the first update may not load every table
        upd()
        got = r.q_or(counts)
    if r.mode == "integrated" and got == TAB.join(["10", "0", "10", "10"]):
        r.observe("keyless-cdc-integrated-snapshot", "dtix (no primary key, CDC) has 0 rows after the first load; the "
                  "keyed tables have 10. No error. Its later claims are skipped")
        skip_dtix = True
    elif got != TAB.join(["10"] * 4):
        r.claim("baseline", "FAIL", f"row counts (intpk dtix cdcpk afrpk): {got}")
        return
    r.observe("scd2-baseline", f"rows scd2pk (SCD_TYPE_2): {r.q_or(f'SELECT COUNT(*) FROM {t}.scd2pk')}")

    # C1: nullable, no default
    db.sql("""ALTER TABLE lfcddemo.intpk ADD c1 INT NULL;
ALTER TABLE lfcddemo.dtix ADD c1 INT NULL;
ALTER TABLE lfcddemo.cdcpk ADD c1 INT NULL;
ALTER TABLE lfcddemo.scd2pk ADD c1 INT NULL;
GO
""" + "".join(f"INSERT INTO lfcddemo.{x} (id, v, c1) VALUES (11, 'after_c1', 111), (12, 'after_c1', 112);\n"
              for x in ("intpk", "dtix", "cdcpk", "scd2pk")))
    settle("intpk" if skip_dtix else "dtix", "c1")
    r.observe("c1-update-chain", f"the updates that ran, with their cause and state: {r.chain}; errors: "
              f"{' '.join(r.errs) or 'none'}")
    want = TAB.join(["10", "2"])
    got = r.expect(want, f"SELECT SUM(CASE WHEN c1 IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN c1 > 110 THEN 1 ELSE 0 END) "
                         f"FROM {t}.intpk")
    r.check("nullable-no-default-keyed-inline", got == want,
            "intpk: 10 rows c1 NULL, 2 rows with values" + ("" if skip_dtix else ", while dtix failed in the same update"),
            f"intpk c1 NULL/values: {got or 'query failed'}")
    if skip_dtix:
        r.observe("keyless-any-add-full-refresh", f"skipped: dtix didn't load; errors for dtix: {' '.join(r.errs_for('dtix'))}")
    else:
        r.check("keyless-any-add-full-refresh", r.has("PRIMARY_KEY_CHANGE"),
                "dtix: PRIMARY_KEY_CHANGE on a nullable no-default add", f"dtix errors: {' '.join(r.errs) or 'none'}")
    got = r.expect(want, f"SELECT SUM(CASE WHEN c1 IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN c1 > 110 THEN 1 ELSE 0 END) "
                         f"FROM {t}.cdcpk")
    r.check("keyed-cdc-table", got == want, "cdcpk (CDC, primary key): 10 rows c1 NULL, 2 rows with values",
            f"cdcpk c1 NULL/values: {got or 'query failed'}")
    e = " ".join(r.errs_for("scd2pk")) or "none"
    got = r.expect(TAB.join(["12", "10", "2"]), f"SELECT COUNT(*), SUM(CASE WHEN c1 IS NULL THEN 1 ELSE 0 END), "
                                                f"SUM(CASE WHEN c1 > 110 THEN 1 ELSE 0 END) FROM {t}.scd2pk")
    r.observe("scd2pk-nullable-add", f"errors: {e}; rows, c1 NULL, c1 set: {got.replace(TAB, ' ')[:120]}")
    fr = ([] if skip_dtix else ["dtix"]) + (["scd2pk"] if r.needs_refresh("scd2pk") else [])
    if fr:
        upd(fr)

    # C2: NOT NULL DEFAULT, then a full refresh
    db.sql("".join(f"ALTER TABLE lfcddemo.{x} ADD c2 VARCHAR(10) NOT NULL DEFAULT 'x';\n"
                   for x in ("intpk", "dtix", "cdcpk", "afrpk", "scd2pk")) + "GO\n" +
           "".join(f"INSERT INTO lfcddemo.{x} (id, v, c1, c2) VALUES (13, 'after_c2', 113, 'y');\n"
                   for x in ("intpk", "dtix", "cdcpk")) +
           "INSERT INTO lfcddemo.afrpk (id, v, c2) VALUES (11, 'after_c2', 'y');\n"
           "INSERT INTO lfcddemo.scd2pk (id, v, c1, c2) VALUES (13, 'after_c2', 113, 'y');\n")
    settle("intpk", "c2")
    r.check("not-null-default-full-refresh", r.has("ADD_COLUMN_WITH_BACKFILL"), "intpk: ADD_COLUMN_WITH_BACKFILL",
            f"errors: {' '.join(r.errs) or 'none'}")
    r.observe("scd2pk-not-null-default-add", f"errors: {' '.join(r.errs_for('scd2pk')) or 'none'}")
    fr = ["intpk", "cdcpk"] + ([] if skip_dtix else ["dtix"]) + (["scd2pk"] if r.needs_refresh("scd2pk") else [])
    upd(fr)                                       # afrpk is never selected: its policy has to refresh it
    r.observe("scd2pk-after-refresh", "rows, c2 = 'x', c2 = 'y': " + r.q_or(
        f"SELECT COUNT(*), SUM(CASE WHEN c2 = 'x' THEN 1 ELSE 0 END), SUM(CASE WHEN c2 = 'y' THEN 1 ELSE 0 END) FROM "
        f"{t}.scd2pk").replace(TAB, " ")[:120])
    want = TAB.join(["12"] * 3)
    got = r.expect(want, f"SELECT (SELECT SUM(CASE WHEN c2 = 'x' THEN 1 ELSE 0 END) FROM {t}.intpk), (SELECT SUM(CASE "
                         f"WHEN c2 = 'x' THEN 1 ELSE 0 END) FROM {t}.{dt}), (SELECT SUM(CASE WHEN c2 = 'x' THEN 1 ELSE 0 "
                         f"END) FROM {t}.cdcpk)")
    r.check("full-refresh-recovers", r.ok_state() and got == want,
            "after a full refresh, 12 old rows carry the default in each table",
            f"full refresh {r.state}, old rows with the default: {got or 'query failed'}")
    want = TAB.join(["10", "11"])
    for _ in range(3):                            # the policy's refresh may land in a later update
        got = r.q_or(f"SELECT SUM(CASE WHEN c2 = 'x' THEN 1 ELSE 0 END), COUNT(*) FROM {t}.afrpk")
        if got == want:
            break
        upd()
    r.check("auto-full-refresh-policy", got == want,
            "afrpk refreshed with no full refresh requested: 10 old rows carry the default, 11 rows",
            f"afrpk default/rows: {got or 'query failed'}; last errors: {' '.join(r.errs) or 'none'}")

    # C3: nullable WITH default
    db.sql("ALTER TABLE lfcddemo.intpk ADD c3 INT NULL DEFAULT 5;\nGO\n"
           "INSERT INTO lfcddemo.intpk (id, v, c1) VALUES (14, 'after_c3', 114);\n")
    settle("intpk", "c3")
    r.check("nullable-default-full-refresh", r.has("ADD_COLUMN_WITH_BACKFILL"),
            "intpk: ADD_COLUMN_WITH_BACKFILL although old source rows stay NULL", f"errors: {' '.join(r.errs) or 'none'}")
    upd(["intpk"])
    want = TAB.join(["13", "1"])
    got = r.expect(want, f"SELECT SUM(CASE WHEN c3 IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN c3 = 5 THEN 1 ELSE 0 END) "
                         f"FROM {t}.intpk")
    if got != want:
        r.claim("full-refresh-recovers", "FAIL", f"after C3 refresh, c3 NULL/5: {got or 'query failed'}")

    # C4: identity, C5: computed
    for i, col in enumerate(("c4 INT IDENTITY(1,1)", "c5 AS (id * 2)")):
        name, row = col.split()[0], 15 + i
        db.sql(f"ALTER TABLE lfcddemo.intpk ADD {col};\nGO\nINSERT INTO lfcddemo.intpk (id, v, c1) VALUES ({row}, "
               f"'after_{name}', 1{row});\n")
        settle("intpk", name)
        r.check("identity-computed-full-refresh", r.has("ADD_COLUMN_WITH_BACKFILL"),
                f"intpk: ADD {col}: ADD_COLUMN_WITH_BACKFILL", f"ADD {col}: errors {' '.join(r.errs) or 'none'}")
        upd(["intpk"])
    r.result_md("sqlserver-add-column")
