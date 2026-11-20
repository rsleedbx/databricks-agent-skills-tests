"""Offline tests of lfc_source_datagen.sources and the per-model modules (no database): sources parsed and refused (and
TPC-H's own checks), the SQL files split and their psql variables filled (casts kept), each model's DDL (tpc_schema for
tpch and tpcb, statschema's shape for the others), a new source's steps in order on a fake DB-API connection (guarded
DDL, the reader's grants before the fill, the checks qualified and marked once, the model's steps with search_path,
the rename last, the publication over every source), a resumed copy skipping its marked checks, a finished source
kept, broken rules stopping before the rename, and the command line refusing bad arguments before connecting.

  uv run --no-project --with 'psycopg[binary]' --with pyyaml --with sqlglot python -m unittest test_sources.py
"""
from __future__ import annotations

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
from lfc_source_datagen import tpcc_sources as tc
from lfc_source_datagen import tpcds_sources as tds
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


class TpccDelivery(unittest.TestCase):
    """TPC-C's delivery times: the parameters' form, the SQL filled, the signal in order_line's comment, the summary
    reading the promise from it; and order_line's item now a foreign key (the backorder cause needs it)."""
    D = "3,0,2,5,2,1,2,12,3,2"

    def test_check_refuses_bad_forms(self):
        tc.check(src.parse([f"a:1:{self.D}", "b:1"]))
        for bad in ("a:1:1,2,3", "a:1:3,0,0,5,2,1,2,12,3,2", "a:1:3,0,2,0,2,1,2,12,3,2", "a:1:3,0,2,5,2,1,2,12,3,0"):
            with self.assertRaises(ValueError, msg=bad):
                tc.check(src.parse([bad]))

    def test_steps_fill_every_variable_and_keep_the_signal(self):
        self.assertEqual(tc.steps("a", ""), [])
        steps = tc.steps("a", self.D)
        body = "\n".join(steps)
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")                      # no psql variable left
        self.assertEqual(len(steps), 4)                                      # open orders x2, the delivery, the comment
        self.assertIn("o.o_d_id % 5 = 0 THEN 2", body)
        self.assertIn("((o.o_carrier_id - 1) % 3) * 1", body)
        self.assertIn("s.s_quantity < 12", body)
        comment = re.search(r"COMMENT ON TABLE order_line IS '(.*)'", body).group(1)
        self.assertEqual(json.loads(comment)["parameters"]["promise"], 3)
        self.assertEqual(json.loads(comment)["signal"], "order_late_delivery")

    def test_summary_reads_the_promise_from_the_comment(self):
        class Cur:
            def __init__(self, comment):
                self.comment, self.q, self.rows = comment, [], []

            def execute(self, q, params=None):
                self.q.append(q)
                self.rows = [(self.comment,)] if "obj_description" in q else [(21000, 39.9, 3.0)]

            def fetchall(self):
                return self.rows
        cur = Cur(tc.signal_comment(self.D))
        self.assertEqual(tc.summary(cur, "local"), {"orders": 21000, "late_pct": 39.9, "median_days": 3.0})
        self.assertIn("d > 3", cur.q[1])
        self.assertEqual(tc.summary(Cur(None), "local")["orders"], None)    # a source without delivery times

    def test_order_line_item_is_a_foreign_key(self):
        ddl, _ = src.model_ddl("tpcc", 0.01)
        self.assertRegex(ddl, r'FOREIGN KEY \("ol_i_id"\) REFERENCES "item"')


class TpcdsSignals(unittest.TestCase):
    """TPC-DS's two signals: the <returns>/<ship> form, either half alone, the SQL filled, each signal in its table's
    comment, the summary reading them back; and both signals' parameters as statschema's tpcds.yaml names them."""
    R, S = "40,80,40,15,1,30", "6,1,3,2,5,3,80,2,3"

    def test_check_refuses_bad_forms(self):
        tds.check(src.parse([f"a:1:{self.R}/{self.S}", f"b:1:{self.R}/", f"c:1:/{self.S}", "d:1"]))
        for bad in (f"a:1:{self.R}", "a:1:1,2,3/", f"a:1:40,80,40,15,1,0/", f"a:1:/6,1,0,2,5,3,80,2,3",
                    f"a:1:/6,1,3,2,0,3,80,2,3", f"a:1:{self.R}/{self.S}/1", "a:1:x/"):
            with self.assertRaises(ValueError, msg=bad):
                tds.check(src.parse([bad]))

    def test_steps_fill_every_variable_and_keep_each_signal(self):
        self.assertEqual(tds.steps("a", ""), [])
        both = tds.steps("a", f"{self.R}/{self.S}")
        body = "\n".join(both)
        self.assertNotRegex(body, r"(?<![:\w]):[a-z_]+")                     # no psql variable left
        self.assertEqual(len(both), 5)       # returns: delete, insert, comment; ship: update, comment
        self.assertIn("s.h % 1000 < 40 + s.is_fit * 80 + s.is_promo * 40 + s.tier * 15", body)
        self.assertIn("ws.ws_warehouse_sk % 5 = 0 THEN 3", body)
        self.assertIn("ws.ws_quantity >= 80 THEN 2", body)
        comments = dict(re.findall(r"COMMENT ON TABLE (\w+) IS '(.*)'", body))
        self.assertEqual(json.loads(comments["store_returns"])["signal"], "return_propensity")
        self.assertEqual(json.loads(comments["web_sales"])["parameters"]["promise"], 6)
        self.assertEqual(len(tds.steps("a", f"/{self.S}")), 2)
        self.assertEqual(len(tds.steps("a", f"{self.R}/")), 3)

    def test_summary_reads_each_signal_from_its_comment(self):
        class Cur:
            def __init__(self, comments):
                self.comments, self.q, self.rows = comments, [], []

            def execute(self, q, params=None):
                self.q.append(q)
                if "obj_description" in q:
                    self.rows = [(self.comments.get(re.search(r"\.(\w+)'::regclass", q).group(1)),)]
                else:
                    self.rows = [(57608, 5644)] if "store_returns" in q else [(14388, 41.0, 6.0)]

            def fetchall(self):
                return self.rows
        both = Cur({"store_returns": tds.signal_comment("return_propensity", self.R),
                    "web_sales": tds.signal_comment("web_ship_delay", self.S)})
        self.assertEqual(tds.summary(both, "a"), {"store_lines": 57608, "returned_pct": 9.8, "web_lines": 14388,
                                                  "late_pct": 41.0, "median_ship_days": 6.0})
        self.assertTrue(any("d > 6" in q for q in both.q))
        self.assertEqual(set(tds.summary(Cur({}), "a").values()), {None})   # a source without either signal

    def test_parameters_match_the_schema(self):
        import yaml
        doc = yaml.safe_load(open(os.path.join(HERE, "..", "..", "lfc-source-statschema", "schemas", "tpcds.yaml")))
        sig = {s["name"]: s for s in doc["signals"]}
        self.assertEqual(list(sig), list(tds.SIGNALS))
        for name, (_t, _sql, names, _) in tds.SIGNALS.items():
            self.assertEqual(tuple(sig[name]["parameters"]), names)
        returns, ship = (sig[n]["profiles"] for n in tds.SIGNALS)
        tds.check(src.parse([f"{r}_{s}:1:{returns[r]}/{ship[s]}" for r in returns for s in ship]))


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
