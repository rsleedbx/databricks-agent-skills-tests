"""Offline tests of lfc_source_contract.cdc on a fake DB-API cursor: the publication created FOR TABLE the schemas'
tables, kept when it has them all (by schema or table), the missing tables added, a failed add explained; the slot
created only when missing, dropped when not active.

  python3 -m unittest test_cdc.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from lfc_source_contract import cdc


class Cur:
    def __init__(self, pub=None, slot=False, add_fails=False):
        self.pub, self.slot, self.add_fails, self.log, self.rows = pub, slot, add_fails, [], []

    def execute(self, q, params=None):
        self.log.append(q)
        if "FROM pg_class c" in q:
            self.rows = [(s, t) for s in params[0].strip("{}").split(",") for t in ("lineitem", "orders")]
        elif "current_setting('server_version_num')" in q:
            self.rows = [("170000",)]
        elif "FROM pg_publication_rel pr" in q and params:
            self.rows = [tuple(x.split(".")) for x in (self.pub or [])]
        elif "FROM pg_publication WHERE pubname = %s" in q:
            self.rows = [(1,)] if self.pub is not None else []
        elif "ADD TABLE" in q and self.add_fails:
            raise RuntimeError("must be owner of publication p")
        elif "FROM pg_replication_slots WHERE slot_name = %s" in q:
            self.rows = [(params[0], False, "1 kB")] if self.slot or any("pg_create_logical" in x for x in self.log) else []
        else:
            self.rows = []

    def fetchall(self):
        return self.rows


class Cdc(unittest.TestCase):
    def test_created_for_table(self):
        c = Cur()
        self.assertEqual(cdc.publication(c, "p", ["a", "b"], log=lambda m: None), "created")
        ddl = next(q for q in c.log if "CREATE PUBLICATION" in q)
        self.assertIn('CREATE PUBLICATION "p" FOR TABLE "a"."lineitem", "a"."orders", "b"."lineitem", "b"."orders"', ddl)
        self.assertTrue(ddl.startswith("DO "))                              # guarded: a rerun leaves it alone

    def test_kept_and_added(self):
        c = Cur(pub=["a.lineitem", "a.orders", "b.lineitem", "b.orders", "x.other"])
        self.assertEqual(cdc.publication(c, "p", ["b", "a"], log=lambda m: None), "kept")
        self.assertFalse([q for q in c.log if "PUBLICATION \"p\"" in q])
        c = Cur(pub=["a.lineitem"])
        self.assertEqual(cdc.publication(c, "p", ["a"], log=lambda m: None), "added 1")
        self.assertTrue(any('ALTER PUBLICATION "p" ADD TABLE "a"."orders"' in q for q in c.log))
        add = next(q for q in c.log if "ADD TABLE" in q)
        self.assertIn("FROM pg_publication_rel pr", add)                    # a partitioned root is a member by name
        self.assertIn("pr.prrelid = '\"a\".\"orders\"'::regclass", add)
        with self.assertRaisesRegex(RuntimeError, "belongs to another login"):
            cdc.publication(Cur(pub=["a.lineitem"], add_fails=True), "p", ["a"], log=lambda m: None)
        with self.assertRaises(ValueError):
            cdc.publication(Cur(), "p", [])

    def test_needs_autocommit(self):
        c = Cur()
        c.connection = type("C", (), {"autocommit": False})()
        for f in (lambda: cdc.publication(c, "p", ["a"], log=lambda m: None), lambda: cdc.slot(c, "s")):
            with self.assertRaisesRegex(ValueError, "autocommit"):
                f()
        self.assertEqual(c.log, [])

    def test_slot(self):
        c = Cur()
        self.assertEqual(cdc.slot(c, "s")["slot"], "s")
        self.assertTrue(any("IF NOT EXISTS" in q and "pg_create_logical_replication_slot('s', 'pgoutput')" in q for q in c.log))
        c = Cur(slot=True)
        cdc.slot(c, "s", drop=True)
        self.assertTrue(any("pg_drop_replication_slot" in q and "NOT active" in q for q in c.log))


class Names(unittest.TestCase):
    def test_a_new_pair_each_time_in_the_lfc_form(self):
        (s1, p1), (s2, _) = cdc.names(), cdc.names()
        self.assertNotEqual(s1, s2)
        self.assertRegex(s1, r"^lfc_[0-9a-f]{8}(_[0-9a-f]{4}){3}_[0-9a-f]{12}$")
        self.assertEqual(p1, "lfc_pub_" + s1[4:])
        self.assertLessEqual(len(p1), 63)   # PostgreSQL's name limit

    def test_a_given_id_and_bad_ones(self):
        self.assertEqual(cdc.names("AB-12"), ("lfc_ab_12", "lfc_pub_ab_12"))
        for bad in ("a b", "x;drop", "x" * 51):
            with self.assertRaises(ValueError):
                cdc.names(bad)


class PipelineSlot(unittest.TestCase):
    class SlotCur(Cur):
        """A slot that drop removes unless it's active."""
        def __init__(self, active=False, **kw):
            super().__init__(slot=True, **kw)
            self.active = active

        def execute(self, q, params=None):
            super().execute(q, params)
            if "FROM pg_replication_slots WHERE slot_name = %s" in q:
                dropped = any("pg_drop_replication_slot" in x for x in self.log)
                self.rows = [] if dropped and not self.active else [(params[0], self.active, "1 kB")]

    quiet = staticmethod(lambda m: None)

    def test_create_prints_the_slot_config(self):
        out = cdc.pipeline_slot(Cur(), "create", id="abc", schemas=["a"], log=self.quiet)
        self.assertEqual(out["slot_config"], {"postgres": {"slot_config": {"slot_name": "lfc_abc", "publication_name": "lfc_pub_abc"}}})
        self.assertEqual(cdc.names(out["id"])[0], out["slot"])              # the printed id gives back the slot

    def test_create_with_a_given_publication(self):
        c = Cur(pub=["a.lineitem", "a.orders"])
        out = cdc.pipeline_slot(c, "create", id="abc", schemas=["a"], publication_name="given_pub", log=self.quiet)
        self.assertEqual(out["slot_config"]["postgres"]["slot_config"]["publication_name"], "given_pub")
        self.assertFalse([q for q in c.log if "CREATE PUBLICATION" in q])   # kept: it has the tables

    def test_a_given_publication_is_only_checked(self):
        for c, msg in ((Cur(), "doesn't exist"), (Cur(pub=["a.lineitem"]), "lacks a.orders")):
            with self.assertRaisesRegex(ValueError, msg):
                cdc.pipeline_slot(c, "create", id="abc", schemas=["a"], publication_name="given_pub", log=self.quiet)
            self.assertFalse([q for q in c.log if "PUBLICATION \"given_pub\"" in q or "pg_create_logical" in q])

    def test_drop_own_and_given(self):
        c = self.SlotCur()
        cdc.pipeline_slot(c, "drop", id="abc", log=self.quiet)
        self.assertTrue(any("pg_drop_replication_slot" in q and "'lfc_abc'" in q for q in c.log))
        self.assertTrue(any('DROP PUBLICATION "lfc_pub_abc"' in q and q.startswith("DO ") for q in c.log))
        c = self.SlotCur()
        cdc.pipeline_slot(c, "drop", id="abc", publication_name="given_pub", log=self.quiet)
        self.assertFalse([q for q in c.log if "DROP PUBLICATION" in q])

    def test_active_slot_stops_the_drop(self):
        c = self.SlotCur(active=True)
        with self.assertRaisesRegex(RuntimeError, "active"):
            cdc.pipeline_slot(c, "drop", id="abc", log=self.quiet)
        self.assertFalse([q for q in c.log if "DROP PUBLICATION" in q])

    def test_bad_steps(self):
        for kw in ({"step": "x"}, {"step": "drop"}):
            with self.assertRaises(ValueError):
                cdc.pipeline_slot(Cur(), **kw)


if __name__ == "__main__":
    unittest.main()
