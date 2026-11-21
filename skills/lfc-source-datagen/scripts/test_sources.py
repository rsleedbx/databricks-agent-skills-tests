"""Offline tests of lfc_source_datagen.sources and the per-model modules (no database): sources parsed and refused (and
TPC-H's own checks), the SQL files split and their psql variables filled (casts kept), each model's DDL (tpc_schema for
tpch and tpcb, statschema's shape for the others), a new source's steps in order on a fake DB-API connection (guarded
DDL, the reader's grants before the fill, the checks qualified and marked once, the model's steps with search_path,
the rename last, the publication over every source), a resumed copy skipping its marked checks, a finished source
kept, broken rules stopping before the rename, and the command line refusing bad arguments before connecting.

  uv run --no-project --with 'psycopg[binary]' --with pyyaml --with sqlglot python -m unittest test_sources.py
"""
from __future__ import annotations

import decimal
import io
import json
import os
import re
import sys
import unittest
from contextlib import redirect_stderr
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (HERE, os.path.join(HERE, "..", "..", "lfc-source-statschema", "scripts"),
          os.path.join(HERE, "..", "..", "lfc-source-contract", "scripts")):
    sys.path.insert(0, p)
from lfc_source_datagen import sources as src
from lfc_source_datagen import signals as sg
from lfc_source_datagen import tpch_sources as tp


class Db:
    """A fake source: answers the catalog questions build() asks; logs every statement."""
    def __init__(self, existing=(), checked=False, bad=0):
        self.existing, self.checked, self.bad, self.log = set(existing), checked, bad, []

    def connect(self):
        db = self

        class Cur:
            def __init__(self):
                self.rows = []

            def execute(self, q, params=None):
                db.log.append(q)
                self.rows = db.answer(q, params)

            def fetchall(self):
                return list(self.rows)

        class Con:
            def cursor(self):
                return Cur()

            def close(self):
                pass
        return Con()

    def answer(self, q, params):
        if q == "SELECT current_database()":
            return [("orders",)]
        if "FROM pg_namespace WHERE nspname = %s" in q and "obj_description" not in q:
            return [(1,)] if params[0] in self.existing else []
        if "obj_description" in q:
            return [(src.CHECKED if self.checked else None,)]
        if q.startswith("SELECT 'bad"):
            return [("bad: x", self.bad)]
        if q.startswith("SELECT '"):
            return [("info", 1)]
        if "lineitem" in q and "PERCENTILE_CONT" in q:
            return [(100, 12.5, 3.0)]
        return []


def build_tpch(db, sources, **k):
    with mock.patch.object(src, "_fill") as fill, mock.patch.object(src.cdc, "publication") as pub:
        rows = tp.tpch_sources(db.connect, sources, log=lambda m: db.log.append(f"LOG {m}"), **k)
    return rows, fill, pub


class Parse(unittest.TestCase):
    def test_forms(self):
        want = [("a", 0.05, "1,0,1,1,5,0,0,1,2"), ("b", 0.1, "")]
        self.assertEqual(src.parse({"a": (0.05, "1,0,1,1,5,0,0,1,2"), "b": 0.1}), want)
        self.assertEqual(src.parse(["a:0.05:1,0,1,1,5,0,0,1,2", "b:0.1"]), want)
        self.assertEqual(src.parse(want), want)                               # its own output

    def test_refused(self):
        for bad in (["Bad:1"], ["a:x"], ["a:0"], ["a:0.03", "a:0.03"], []):
            with self.assertRaises(ValueError, msg=bad):
                src.parse(bad)
        for bad in (["a:0.01"], ["a:0.03:1,2"], ["a:0.03:1,0,0,1,5,0,0,1,2"]):
            with self.assertRaises(ValueError, msg=bad):
                tp.check(src.parse(bad))


class SqlFiles(unittest.TestCase):
    def test_verify_split(self):
        for m in ("tpch", "tpcb"):
            text = src.read_sql(f"{m}-verify.sql")
            self.assertEqual(len(src.statements(text)), len(re.findall(r"^SELECT '", text, re.MULTILINE)), m)

    def test_lead_times_and_labels(self):
        steps = tp.steps("anonymous")("a", "1,2,3,4,5,6,7,8,9")
        body = "\n".join(steps)
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")
        self.assertIn("CASE WHEN 'anonymous' = 'spec'", body)
        self.assertIn(")::int AS ship_d", body)
        self.assertIn("od + 1, l_shipdate", body); self.assertIn("% 5 = 0 THEN 4", body)
        self.assertIn("WHEN 'AIR' THEN 6", body); self.assertIn("ELSE 7 END", body); self.assertIn("'SHIP' THEN 8", body)
        self.assertEqual(len(tp.steps("spec")("a", "")), 6)                    # labels only
        with self.assertRaises(ValueError):
            tp.steps("other")

    def test_model_ddl(self):
        ddl, tables = src.model_ddl("tpcb", 1)
        self.assertEqual(sorted(tables), ["account", "branch", "history", "teller"])
        self.assertIn('CREATE TABLE "branch"', ddl)
        ddl, tables = src.model_ddl("tpcc", 0.01)                             # statschema's model, shaped
        self.assertIn("warehouse", tables); self.assertIn('CREATE TABLE "warehouse"', ddl)


class Cur:
    """A cursor for summary(): each table's comment (obj_description) from `comments`, any other query `rows`."""
    def __init__(self, comments, rows=()):
        self.comments, self.answers, self.q, self.rows = comments, list(rows), [], []

    def execute(self, q, params=None):
        self.q.append(q)
        if "obj_description" in q:
            self.rows = [(self.comments.get(re.search(r"\.\"?(\w+)\"?'::regclass", q).group(1)),)]
        else:
            self.rows = [self.answers.pop(0)]

    def fetchall(self):
        return self.rows


def tagged(body: str) -> dict:
    """{table: the signal JSON} from steps' comment statements."""
    return {t: json.loads(j) for t, j in re.findall(r"%L', '(\w+)'.*?'lfc:signal=(\{.*?\})'\)\); END", body)}


class SignalComment(unittest.TestCase):
    """The signal goes into the table's comment after the fill's tags, which a later fill reads (lfc:rows, lfc:n,
    lfc:by); a comment that is only the JSON (a source built before) still reads; another signal's doesn't."""

    def test_parameters_read_after_the_fill_tags(self):
        j = sg.comment("tpcc", "order_late_delivery", TpccDelivery.D)
        for text in (f"lfc:rows=300 lfc:n={{(ol_w_id - 1) * 30 + 1}} lfc:by={{ol_w_id}} {sg.TAG}{j}", j, f"{sg.TAG}{j}"):
            self.assertEqual(sg.parameters(text, "order_late_delivery")["promise"], 3, text)
        for text in (None, "", "lfc:rows=300", f"lfc:rows=3 {sg.TAG}{j}x"):
            self.assertIsNone(sg.parameters(text, "order_late_delivery"), text)
        self.assertIsNone(sg.parameters(j, "web_ship_delay"))

    def test_the_statement_keeps_the_tags_and_replaces_an_old_signal(self):
        s = sg.tag("trade", '{"signal":"x"}')
        self.assertTrue(s.startswith("DO $sig$") and s.endswith("END $sig$"))
        self.assertIn("""regexp_replace(COALESCE(obj_description(to_regclass('"trade"'), 'pg_class'), ''), ' *lfc:signal=.*$', '')""", s)
        self.assertIn("""to_regclass('"DimAccount"')""", sg.tag("DimAccount", "{}"))   # a mixed-case name, quoted
        self.assertIn("""'lfc:signal={"signal":"x"}'""", s)


class TpceTrades(unittest.TestCase):
    """TPC-E's trade failures keep the trade's other rows consistent: no holding, holding history or cash transaction
    of a cancelled trade, and cash transactions only for completed cash trades."""

    def test_the_other_rows_of_a_cancelled_trade_go(self):
        body = "\n".join(sg.steps("tpce", "a", "8,0,3,3,5,4,1,2,3,50,30,60,5"))
        for tbl in ("settlement", "holding_history", "holding", "cash_transaction"):
            self.assertRegex(body, rf"DELETE FROM {tbl} \w+ USING trade t", tbl)
        self.assertIn("t.t_st_id = 'CNCL' OR t.t_is_cash = 0", body)


class Reapply(unittest.TestCase):
    """Signals re-applied on a source's rows now (after an append): fragments in the SQL files, each signal's own
    parameters from its comment, the rows each statement changed; lfc-sources --append names the sources."""

    def test_fragments(self):
        self.assertEqual(sg.expand("-- fragment w\nSELECT 1\n-- end\nDELETE x WHERE NOT IN ({{w}});\nINSERT ({{w}});\n"),
                         "DELETE x WHERE NOT IN (SELECT 1);\nINSERT (SELECT 1);\n")
        with self.assertRaises(ValueError):
            sg.expand("SELECT {{nope}}")
        body = "\n".join(sg.steps("tpcds", "a", f"{TpcdsSignals.R}/"))
        self.assertEqual(body.count("FROM (WITH r AS ("), 6)                    # each channel's wanted returns, delete and insert

    def test_reapply_reads_each_signal_and_counts(self):
        class Cur(globals()["Cur"]):
            rowcount = 7

            def execute(self, q, params=None):
                if q.startswith("SELECT obj_description"):
                    return super().execute(q, params)
                self.q.append(q)
                self.rows = []
        cur = Cur({"web_sales": sg.comment("tpcds", "web_ship_delay", TpcdsSignals.S)}, [])
        got = sg.reapply(cur, "tpcds", "a", log=lambda m: None)
        self.assertEqual(list(got), ["web_ship_delay"])                       # store_returns has no signal comment
        self.assertEqual(got["web_ship_delay"], [7, 0])                         # the update, then the comment (none)
        self.assertTrue(any("ws.ws_quantity >= 80 THEN 2" in q for q in cur.q))  # its own parameters

    def test_append_leaves_rebuilt_tables_to_the_signal(self):
        cur = type("C", (), {"execute": lambda s, q, p=None: None, "fetchall": lambda s: [(1,)], "close": lambda s: None})()
        con = type("K", (), {"cursor": lambda s: cur, "close": lambda s: None})()
        with mock.patch.object(sg, "_fill") as fill, mock.patch.object(sg, "reapply", return_value={}), \
                mock.patch.object(sg, "summary", return_value={}):
            sg.append(lambda: con, "tpcds", ["a"], 0.01, log=lambda m: None)
        extra = fill.call_args.args[3]
        self.assertEqual(extra[:4], ("--mode", "append", "--grow", "0.01"))
        self.assertEqual(extra[extra.index("--skip") + 1], "catalog_returns,store_returns,web_returns")

    def test_append_option_names_the_sources(self):
        with mock.patch("lfc_source_datagen.signals.append", return_value=[{"source": "a", "changed": 0}]) as ap, \
                mock.patch.object(src, "connector", return_value=lambda: None), redirect_stderr(io.StringIO()):
            rc = src.main(["--model", "tpcds", "--append", "5", "--host", "h", "--password-file", __file__, "db", "a", "b:1"])
        self.assertEqual(rc, 0)
        self.assertEqual(ap.call_args.args[1:4], ("tpcds", ["a", "b"], 5))


class TpccDelivery(unittest.TestCase):
    """TPC-C's delivery times through lfc_source_datagen.signals: the parameters' form, the SQL filled, the signal in
    order_line's comment, the summary reading the promise from it; and order_line's item a foreign key (the backorder
    cause needs it)."""
    D = "3,0,2,5,2,1,2,12,3,2"

    def test_check_refuses_bad_forms(self):
        sg.check("tpcc", src.parse([f"a:1:{self.D}", "b:1"]))
        for bad in ("a:1:1,2,3", "a:1:3,0,0,5,2,1,2,12,3,2", "a:1:3,0,2,0,2,1,2,12,3,2", "a:1:3,0,2,5,2,1,2,12,3,0"):
            with self.assertRaises(ValueError, msg=bad):
                sg.check("tpcc", src.parse([bad]))

    def test_steps_fill_every_variable_and_keep_the_signal(self):
        self.assertEqual(sg.steps("tpcc", "a", ""), [])
        steps = sg.steps("tpcc", "a", self.D)
        body = "\n".join(steps)
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")                      # no psql variable left
        self.assertEqual(len(steps), 4)                                      # open orders x2, the delivery, the comment
        self.assertIn("o.o_d_id % 5 = 0 THEN 2", body)
        self.assertIn("((o.o_carrier_id - 1) % 3) * 1", body)
        self.assertIn("s.s_quantity < 12", body)
        self.assertIn("COMMENT ON TABLE %I IS %L', 'order_line'", steps[-1])     # after the step, keeping the fill tags
        got = tagged(body)
        self.assertEqual(got["order_line"]["parameters"]["promise"], 3)
        self.assertEqual(got["order_line"]["signal"], "order_late_delivery")

    def test_summary_reads_the_promise_from_the_comment(self):
        cur = Cur({"order_line": sg.comment("tpcc", "order_late_delivery", self.D)}, [(21000, decimal.Decimal("39.9"), 3.0)])
        self.assertEqual(sg.summary("tpcc", cur, "local"), {"orders": 21000, "late_pct": 39.9, "median_days": 3.0})
        self.assertIn("d > 3", cur.q[1])
        self.assertIn('"local".orders', cur.q[1])                             # {s}: the source's schema
        self.assertEqual(sg.summary("tpcc", Cur({}), "local")["orders"], None)    # a source without delivery times

    def test_order_line_item_is_a_foreign_key(self):
        ddl, _ = src.model_ddl("tpcc", 0.01)
        self.assertRegex(ddl, r'FOREIGN KEY \("ol_i_id"\) REFERENCES "item"')


class TpcdsSignals(unittest.TestCase):
    """TPC-DS's two signals through lfc_source_datagen.signals: the <returns>/<ship> form, either half alone, the SQL
    filled, each signal in its table's comment, the summary reading them back."""
    R, S = "40,80,40,15,1,30", "6,1,3,2,5,3,80,2,3"

    def test_check_refuses_bad_forms(self):
        sg.check("tpcds", src.parse([f"a:1:{self.R}/{self.S}", f"b:1:{self.R}/", f"c:1:/{self.S}", "d:1"]))
        for bad in (f"a:1:{self.R}", "a:1:1,2,3/", "a:1:40,80,40,15,1,0/", "a:1:/6,1,0,2,5,3,80,2,3",
                    "a:1:/6,1,3,2,0,3,80,2,3", f"a:1:{self.R}/{self.S}/1", "a:1:x/"):
            with self.assertRaises(ValueError, msg=bad):
                sg.check("tpcds", src.parse([bad]))

    def test_steps_fill_every_variable_and_keep_each_signal(self):
        self.assertEqual(sg.steps("tpcds", "a", ""), [])
        both = sg.steps("tpcds", "a", f"{self.R}/{self.S}")
        body = "\n".join(both)
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")                     # no psql variable left
        self.assertEqual(len(both), 9)       # returns: delete, insert per channel (3), comment; ship: update, comment
        self.assertIn("s.h % 1000 < 40 + s.is_fit * 80 + s.is_promo * 40 + s.tier * 15", body)
        self.assertIn("ws.ws_warehouse_sk % 5 = 0 THEN 3", body)
        self.assertIn("ws.ws_quantity >= 80 THEN 2", body)
        comments = tagged(body)
        self.assertEqual(comments["store_returns"]["signal"], "return_propensity")
        self.assertEqual(comments["web_sales"]["parameters"]["promise"], 6)
        self.assertEqual(len(sg.steps("tpcds", "a", f"/{self.S}")), 2)
        self.assertEqual(len(sg.steps("tpcds", "a", f"{self.R}/")), 7)
        for tbl in ("store_returns", "catalog_returns", "web_returns"):
            self.assertIn(f"INSERT INTO {tbl}", body)

    def test_summary_reads_each_signal_from_its_comment(self):
        both = Cur({"store_returns": sg.comment("tpcds", "return_propensity", self.R),
                    "web_sales": sg.comment("tpcds", "web_ship_delay", self.S)},
                   [(57608, decimal.Decimal("9.8"), decimal.Decimal("9.7"), decimal.Decimal("9.9")), (14388, decimal.Decimal("41.0"), 6.0)])
        self.assertEqual(sg.summary("tpcds", both, "a"), {"store_lines": 57608, "returned_pct": 9.8, "catalog_returned_pct": 9.7,
                                                          "web_returned_pct": 9.9, "web_lines": 14388, "late_pct": 41.0,
                                                          "median_ship_days": 6.0})
        self.assertTrue(any("d > 6" in q for q in both.q))
        self.assertEqual(set(sg.summary("tpcds", Cur({}), "a").values()), {None})   # a source without either signal
        wrong = Cur({"store_returns": sg.comment("tpcds", "web_ship_delay", self.S)})   # another signal's comment
        self.assertIsNone(sg.summary("tpcds", wrong, "a")["store_lines"])
        short = Cur({"order_line": sg.comment("tpcc", "order_late_delivery", TpccDelivery.D)}, [(1, 2)])
        with self.assertRaises(ValueError):                                   # a row unlike summary_columns
            sg.summary("tpcc", short, "a")


class SignalBlocks(unittest.TestCase):
    """Every model's signals: block that lfc_source_datagen.signals runs: its SQL file is there with a variable for each
    parameter and :'signal', its profiles pass check(), its summary names a column per value; the wrappers' commands
    are lfc-sources --model."""

    def test_every_block_runs(self):
        models = [m for m in ("tpcb", "tpcc", "tpch", "tpcds", "tpce", "tpcdi") if sg.spec(m)]
        self.assertTrue({"tpcb", "tpcc", "tpcds", "tpce", "tpcdi"} <= set(models))
        for m in models:
            sigs = sg.spec(m)
            for s in sigs:
                sql = src.read_sql(s["sql"])
                for k in s["parameters"]:
                    self.assertRegex(sql, rf"(?<![:\w]):{k}\b", f"{m} {s['name']}: :{k} not in {s['sql']}")
                self.assertNotIn("COMMENT ON", sql.upper(), f"{m}: {s['sql']} writes the comment; signals.steps does")
                self.assertTrue(set(s.get("at_least_1") or []) <= set(s["parameters"]))
                self.assertTrue(s["summary_columns"] and "{s}" in s["summary"], f"{m} {s['name']}: summary")
            self.assertEqual(len({s["table"] for s in sigs}), len(sigs), f"{m}: one signal per table")
            profiles = [list(s["profiles"].values()) for s in sigs]
            body = "\n".join(sg.steps(m, "a", "/".join(p[0] for p in profiles)))
            self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+", f"{m}: a psql variable left in its steps")
            self.assertNotIn("{{", body, f"{m}: a fragment left in its steps")
            self.assertEqual(body.count("COMMENT ON TABLE"), len(sigs))
            combos = ["/".join(p[0] if i == j else "" for i, p in enumerate(profiles)) for j in range(len(sigs))]
            sg.check(m, src.parse([f"s{i}:1:{c}" for i, c in enumerate(combos)] + [f"all{i}:1:" + "/".join(p[i % len(p)] for p in profiles) for i in range(3)]))

    def test_lfc_sources_runs_signals_only_with_numbers(self):
        for argv, want in ((["--model", "tpcc", "db", "a:1"], None), (["--model", "tpcc", "db", "a:1:" + TpccDelivery.D], "steps"),
                           (["--model", "tpcc", "--signals", "db", "a:1"], "steps"), (["--model", "tpch", "db", "a:1:1,0,1,1,5,0,0,1,2"], None)):
            with mock.patch.object(src, "run", return_value=0) as run:
                src.main(argv)
            got = run.call_args.args[2]
            self.assertEqual(None if got is None else "steps", want, argv)

    def test_wrappers_are_lfc_sources(self):
        from lfc_source_datagen import tpcc_sources, tpcds_sources
        for mod, m in ((tpcc_sources, "tpcc"), (tpcds_sources, "tpcds")):
            with mock.patch.object(mod, "_sources_main", return_value=0) as run:
                self.assertEqual(mod.main(["db", "a:1"]), 0)
            run.assert_called_once_with(["--model", m, "--signals", "db", "a:1"])


class AppendSkipsFullTables(unittest.TestCase):
    """python/planner.py: an append leaves out a table tagged lfc:fixed (every key value used, shape.py says so), whose
    new rows could only repeat a key; a fill still plans it."""

    def test_append_skips_a_fixed_table_only(self):
        planner = src._planner("planner")
        class eng:                                # only what plan() asks of an engine
            cast = staticmethod(lambda c, n, o: "x.n")
            length = staticmethod(lambda c, o: None)
            to_int = staticmethod(lambda c, q: q)
            q = qby = staticmethod(lambda s: f'"{s}"')
            insert = staticmethod(lambda sch, t, cols, exprs, o: f"INSERT INTO {t.name}")

        def tabs():
            col = lambda: planner.Col("id", 1, "int4", pk=True)
            return [planner.Table("codes", [col()], ["id"], tags="lfc:rows=5 lfc:fixed"),
                    planner.Table("facts", [col()], ["id"], tags="lfc:rows=500")]
        for mode, want in (("append", ["facts"]), ("fill", ["codes", "facts"])):
            ts = tabs()
            planned = sorted(n for _, n, _ in planner.plan(eng, ts, "s", planner.Opts(mode=mode)))
            self.assertEqual(planned, want, mode)
        self.assertEqual(next(x for x in ts if x.name == "codes").skipped, None)
        ts = tabs(); planner.plan(eng, ts, "s", planner.Opts(mode="append"))
        self.assertIn("fixed size", next(x for x in ts if x.name == "codes").skipped)

    def test_grow_by_scale_factor(self):
        planner = src._planner("planner")

        class eng:
            cast = staticmethod(lambda c, n, o: "x.n")
            length = staticmethod(lambda c, o: None)
            to_int = staticmethod(lambda c, q: q)
            q = qby = staticmethod(lambda s: f'"{s}"')
            insert = staticmethod(lambda sch, t, cols, exprs, o: f"INSERT {t.name} {o.rows}")
        col = lambda: planner.Col("id", 1, "int4", pk=True)
        tabs = lambda: [planner.Table("band", [col()], ["id"], tags="lfc:rows=20"),               # fixed size: no tag
                        planner.Table("acct", [col()], ["id"], tags="lfc:rows=1000 lfc:sf_rows=100000"),
                        planner.Table("hist", [col()], ["id"], tags="lfc:rows=1000 lfc:sf_rows=1000000"),
                        planner.Table("rets", [col()], ["id"], tags="lfc:rows=100 lfc:sf_rows=10000")]
        got = {n: s for _, n, s in planner.plan(eng, tabs(), "s", planner.Opts(mode="append", grow=0.01, skip=frozenset({"rets"})))}
        self.assertEqual(got, {"acct": "INSERT acct 1000", "hist": "INSERT hist 10000"})   # each its share; rets skipped
        with self.assertRaisesRegex(ValueError, r"grow by 0.0001 or more"):               # rets would get 0.1 of a row
            planner.plan(eng, tabs(), "s", planner.Opts(mode="append", grow=0.00001))
        tb = lambda: [planner.Table("branch", [col()], ["id"], tags="lfc:rows=1 lfc:sf_rows=1"),
                      planner.Table("teller", [col()], ["id"], tags="lfc:rows=10 lfc:sf_rows=10")]
        with self.assertRaisesRegex(ValueError, r"isn't a whole number of steps of 1"):  # 1.4: 1 branch but 14 tellers
            planner.plan(eng, tb(), "s", planner.Opts(mode="append", grow=1.4))
        self.assertEqual(len(planner.plan(eng, tb(), "s", planner.Opts(mode="append", grow=2))), 2)
        fill = planner.plan(eng, tb(), "s", planner.Opts(mode="fill", targets={"teller": 30}))   # growth as fill rounds
        self.assertEqual([x.target for x in (lambda ts: (planner.plan(eng, ts, "s", planner.Opts(mode="fill", targets={"teller": 30})), ts)[1])(tb())], [1, 30])


class Build(unittest.TestCase):
    def test_new_source_steps_in_order(self):
        db = Db()
        rows, fill, pub = build_tpch(db, ["a:0.03:1,0,1,1,5,0,0,1,2"], login="lfc_cdc", labels="anonymous")
        q = db.log
        ddl = next(i for i, s in enumerate(q) if s.startswith("DO ") and 'CREATE TABLE "lineitem"' in s)
        grant = q.index('GRANT SELECT ON ALL TABLES IN SCHEMA "tpch_0001" TO "lfc_cdc"')
        verify = [s for s in q if s.startswith("SELECT 'bad")]
        mark = q.index(f"COMMENT ON SCHEMA \"tpch_0001\" IS '{src.CHECKED}'")
        path = q.index('SET search_path = "tpch_0001"')
        lead = next(i for i, s in enumerate(q) if s.startswith("WITH d AS"))
        rename = next(i for i, s in enumerate(q) if 'ALTER SCHEMA "tpch_0001" RENAME TO "a"' in s)
        self.assertTrue(ddl < grant < mark < path < lead < rename)
        self.assertTrue(q[ddl].startswith("DO ") and 'SET LOCAL search_path = "tpch_0001"' in q[ddl])
        self.assertTrue(verify and not [s for s in verify if "DATEDIFF(" in s])
        self.assertTrue(all('"tpch_0001".lineitem' in s or "lineitem" not in s for s in verify))
        self.assertIn("COMMENT ON SCHEMA", q[rename])                          # the mark cleared with the rename
        self.assertEqual(fill.call_args.args[1:], ("tpch_0001", 4))
        self.assertEqual(pub.call_args.args[1:], ("orders_pub", ["a"]))
        self.assertEqual(rows, [{"source": "a", "lines": 100, "late_pct": 12.5, "median_days": 3.0}])

    def test_resume_skips_marked_checks_and_kept_source(self):
        db = Db(checked=True)
        build_tpch(db, ["a:0.03"])
        self.assertFalse([s for s in db.log if s.startswith("SELECT 'bad")])
        db = Db(existing={"a"})
        _, fill, _ = build_tpch(db, ["a:0.03"])
        fill.assert_not_called()
        self.assertIn("LOG sources: orders.a exists: kept", db.log)
        self.assertFalse([s for s in db.log if s.startswith("DO ")])

    def test_no_fill_builds_empty_tables_only(self):
        db = Db()
        with mock.patch.object(src, "_fill") as fill, mock.patch.object(src.cdc, "publication") as pub:
            rows = sg.model_sources(db.connect, "tpcb", ["a:1"], login="lfc_cdc", fill=False,
                                    log=lambda m: db.log.append(f"LOG {m}"))
        fill.assert_not_called()
        self.assertTrue([s for s in db.log if s.startswith("DO ") and 'CREATE TABLE "history"' in s])
        self.assertFalse([s for s in db.log if s.startswith("SELECT 'bad") or s.startswith("WITH r AS")])
        self.assertTrue([s for s in db.log if 'RENAME TO "a"' in s])
        self.assertEqual(pub.call_args.args[1:], ("orders_pub", ["a"]))
        self.assertEqual(rows[0]["source"], "a")

    def test_broken_rules_stop_before_the_rename(self):
        db = Db(bad=3)
        with self.assertRaisesRegex(RuntimeError, "spec rules broken"):
            build_tpch(db, ["a:0.03"])
        self.assertFalse([s for s in db.log if "RENAME" in s])


class CommandLine(unittest.TestCase):
    def test_bad_arguments_before_connecting(self):
        with mock.patch.object(src, "connector", side_effect=AssertionError("connected")), \
             redirect_stderr(io.StringIO()) as err:
            self.assertEqual(tp.main(["--lakebase", "p", "db", "Bad:1"]), 2)
            self.assertEqual(tp.main(["--lakebase", "p", "db", "a:0.01"]), 2)
            self.assertEqual(src.main(["--lakebase", "p", "--model", "tpcc", "db", "a:x"]), 2)
        self.assertIn("lower-case letters", err.getvalue()); self.assertIn("0.03 or more", err.getvalue())


if __name__ == "__main__":
    unittest.main()
