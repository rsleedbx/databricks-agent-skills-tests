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
        if "FROM information_schema.tables" in q:
            self.rows = [(s, t) for s in params[0].strip("{}").split(",") for t in ("lineitem", "orders")]
        elif "FROM pg_publication_tables WHERE pubname = %s" in q:
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
        with self.assertRaisesRegex(RuntimeError, "belongs to another login"):
            cdc.publication(Cur(pub=["a.lineitem"], add_fails=True), "p", ["a"], log=lambda m: None)
        with self.assertRaises(ValueError):
            cdc.publication(Cur(), "p", [])

    def test_slot(self):
        c = Cur()
        self.assertEqual(cdc.slot(c, "s")["slot"], "s")
        self.assertTrue(any("IF NOT EXISTS" in q and "pg_create_logical_replication_slot('s', 'pgoutput')" in q for q in c.log))
        c = Cur(slot=True)
        cdc.slot(c, "s", drop=True)
        self.assertTrue(any("pg_drop_replication_slot" in q and "NOT active" in q for q in c.log))


if __name__ == "__main__":
    unittest.main()
