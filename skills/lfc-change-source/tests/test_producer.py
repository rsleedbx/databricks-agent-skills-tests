"""lfc_change_source.producer offline: a split's steps make before break, carry_out's order and the cut, and a Debezium
Server's properties retargeted (tables, log, slot, offsets).

    python3 -m unittest skills/lfc-change-source/tests/test_producer.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lfc_change_source import producer  # noqa: E402
from lfc_change_source.producer import Spec, carry_out, debezium_properties, split  # noqa: E402


class Fake:
    log_ = []

    def __init__(self, spec):
        self.name, self.log, self.tables = spec.name, spec.log, spec.tables

    def position(self):
        Fake.log_.append(("position", self.name))
        return "0/1A2B"

    def stop(self):
        Fake.log_.append(("stop", self.name, self.tables))

    def start(self):
        Fake.log_.append(("start", self.name, self.log, self.tables))


OLD = Spec("dbz1", "c.s.cdc_log", ("s.a", "s.b", "s.c"))


class Split(unittest.TestCase):
    def setUp(self):
        Fake.log_ = []

    def test_make_before_break(self):
        steps = split(OLD, ["s.b"], "c.s.cdc_log_b", Fake, new_name="dbz2")
        self.assertEqual([s.op for s in steps], ["start", "wait_landed", "stop", "start_again"])
        out = carry_out(steps, landed=lambda p: True, wait=lambda s: None)
        self.assertEqual(Fake.log_, [("start", "dbz2", "c.s.cdc_log_b", ("s.b",)),
                                     ("stop", "dbz1", ("s.a", "s.b", "s.c")), ("position", "dbz1"),
                                     ("start", "dbz1", "c.s.cdc_log", ("s.a", "s.c"))])
        self.assertEqual(out, {"moved": ("s.b",), "log": "c.s.cdc_log_b", "cut": "0/1A2B"})

    def test_the_old_producer_runs_on_until_the_new_log_has_a_change(self):
        n = iter([False, False, True])
        carry_out(split(OLD, ["s.a"], "c.s.l2", Fake), landed=lambda p: next(n), wait=lambda s: None)
        self.assertEqual(Fake.log_[0][0], "start")
        self.assertEqual(Fake.log_[1][0], "stop")   # only after the third check

    def test_a_new_log_that_never_lands_leaves_the_old_producer_running(self):
        with self.assertRaises(TimeoutError):
            carry_out(split(OLD, ["s.a"], "c.s.l2", Fake), landed=lambda p: False, wait=lambda s: None)
        self.assertFalse(any(e[0] == "stop" for e in Fake.log_))

    def test_every_table_moved_retires_the_old_producer(self):
        steps = split(OLD, list(OLD.tables), "c.s.l2", Fake)
        self.assertEqual(steps[-1].op, "retire")

    def test_refusals(self):
        for moved, log in ((["s.x"], "c.s.l2"), (["s.a"], OLD.log), ([], "c.s.l2")):
            with self.assertRaises(ValueError):
                split(OLD, moved, log, Fake)


PROPS = """debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.slot.name=dbz_slot_1
debezium.source.table.include.list=s.a,s.b,s.c
debezium.source.offset.storage.file.filename=/debezium/data/offsets.dat
debezium.sink.zerobus.table=c.s.cdc_log
debezium.sink.zerobus.client.secret=@SECRET@
"""


class DebeziumProperties(unittest.TestCase):
    def test_retargeted(self):
        out = debezium_properties(PROPS, tables=["s.b"], log="`c`.`s`.`cdc_log_b`", slot="dbz_slot_1_b",
                                  offsets="/debezium/data/offsets-b.dat")
        self.assertIn("debezium.source.table.include.list=s.b\n", out)
        self.assertIn("debezium.sink.zerobus.table=c.s.cdc_log_b\n", out)
        self.assertIn("debezium.source.slot.name=dbz_slot_1_b\n", out)
        self.assertIn("debezium.source.offset.storage.file.filename=/debezium/data/offsets-b.dat\n", out)
        self.assertIn("client.secret=@SECRET@", out)   # the rest as it was

    def test_only_what_is_given_changes(self):
        self.assertEqual(debezium_properties(PROPS, tables=["s.a", "s.c"]),
                         PROPS.replace("include.list=s.a,s.b,s.c", "include.list=s.a,s.c"))

    def test_no_empty_include_list_and_no_missing_key(self):
        with self.assertRaises(ValueError):
            debezium_properties(PROPS, tables=[])
        with self.assertRaises(ValueError):
            debezium_properties(PROPS.replace("debezium.source.slot.name=dbz_slot_1\n", ""), slot="x")


class Debezium(unittest.TestCase):
    """DebeziumServer's commands on a fake VM: its folder and properties, the container, waiting for streaming."""

    def vm(self, logs):
        seen = []

        def run(cmd):
            seen.append(cmd)
            if cmd.startswith("cat "):
                return PROPS
            if " logs " in cmd:
                return logs.pop(0) if logs else "Starting streaming"
            return ""
        return run, seen

    def test_start_writes_its_properties_runs_and_waits_for_streaming(self):
        run, seen = self.vm(["", "Starting streaming"])
        d = producer.DebeziumServer(Spec("dbz2", "`c`.`s`.`cdc_log_b`", ("s.b",)), run, "/tmp/dbz", "/tmp/dbz/src1/application.properties",
                                    "img:1", "net1", slot="dbz_slot_b", sleep=lambda s: None)
        d.start()
        written = next(c for c in seen if "LFC_PROPS" in c)
        self.assertIn("/tmp/dbz/dbz2/application.properties", written)
        self.assertIn("table.include.list=s.b\n", written)
        self.assertIn("zerobus.table=c.s.cdc_log_b\n", written)
        self.assertIn("slot.name=dbz_slot_b\n", written)
        run_cmd = next(c for c in seen if " run -d " in c)
        self.assertIn("--name dbz2 --network net1", run_cmd)
        self.assertIn("/tmp/dbz/dbz2/data:/debezium/data:U img:1", run_cmd)
        self.assertEqual(sum(" logs " in c for c in seen), 2)

    def test_a_failed_engine_raises(self):
        run, _ = self.vm(["Engine has failed"])
        d = producer.DebeziumServer(Spec("dbz2", "c.s.l", ("s.b",)), run, "/h", "/t", "i", "n", sleep=lambda s: None)
        with self.assertRaises(RuntimeError):
            d.start()

    def test_position_is_the_slot_s(self):
        run, _ = self.vm([])
        d = producer.DebeziumServer(Spec("dbz1", "c.s.l", ("s.a",)), run, "/h", "/t", "i", "n", slot="s1",
                                    position_fn=lambda slot: f"0/AB for {slot}")
        self.assertEqual(d.position(), "0/AB for s1")


if __name__ == "__main__":
    unittest.main()
