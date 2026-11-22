"""Offline checks of lfc_medallion_pack: the 20-70-10 tiers, a project planned into pipelines within the table limit,
render's files, the TPC-B pack's marts against its model, and the mistakes check must catch.

    PYTHONPATH=skills/lfc-medallion-pack/scripts:skills/lfc-source-datagen/scripts:skills/lfc-source-statschema/scripts:\
skills/lfc-source-contract/scripts python3 -m unittest skills/lfc-medallion-pack/tests/test_pack.py
"""
import json
import os
import shutil
import sys
import tempfile
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-medallion", "scripts"))
from lfc_medallion_pack import check, cli, dashboard, lobs, pack, plan, timings, verify  # noqa: E402

COLS = {"branch": ["b_id", "b_balance", "b_filler"], "teller": ["t_id", "t_b_id", "t_balance", "t_filler"],
        "account": ["a_id", "a_b_id", "a_balance", "a_filler"],
        "history": ["h_t_id", "h_b_id", "h_a_id", "h_delta", "h_time", "h_filler"]}


def project(folder, **modules):
    f = os.path.join(folder, "p.yaml")
    with open(f, "w") as fh:
        yaml.safe_dump({"archive": "https://example.invalid/a.zip", "source": {"slot_prefix": "p"},
                        "modules": modules}, fh)
    return f


class Lobs(unittest.TestCase):
    def test_twenty_seventy_ten(self):
        for n, want in ((10, (2, 7, 1)), (250, (50, 175, 25)), (3, (1, 1, 1))):
            got = [x["tier"] for x in lobs.lobs(n, seed=7)]
            self.assertEqual((got.count("high"), got.count("core"), got.count("low")), want)

    def test_seeded_and_ranked(self):
        a, b = lobs.lobs(20, seed=3), lobs.lobs(20, seed=3)
        self.assertEqual(a, b)
        hi = min(x["score"] for x in a if x["tier"] == "high")
        self.assertTrue(all(x["score"] <= hi for x in a if x["tier"] != "high"))

    def test_names(self):
        self.assertEqual([x["name"] for x in lobs.lobs(names=["east", "west"])], ["east", "west"])
        with self.assertRaises(ValueError):
            lobs.lobs(names=["East"])


class Plan(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.d)

    def test_groups_within_the_table_limit(self):
        pl = plan.plan(project(self.d, bank={"pack": "tpcb", "lobs": 250, "seed": 7}))
        m = pl["modules"][0]
        self.assertEqual([len(g["lobs"]) for g in m["groups"]], [62, 62, 62, 62, 2])   # 248 tables: 4 x 62 <= 250
        self.assertTrue(all(g["tables"] <= 250 for g in m["groups"]))
        self.assertEqual(m["groups"][0]["slot"], "p_bank_1")
        self.assertEqual(pl["slots"], 5)
        low = next(x for x in m["lobs"] if x["tier"] == "low")
        self.assertEqual(low["signals"], pack.signals(pack.load("tpcb"), "low"))
        self.assertEqual(low["source_schema"], f"bank_{low['name']}")

    def test_split_by_role_puts_facts_and_dimensions_in_pipelines_of_their_own(self):
        f = project(self.d, bank={"pack": "tpcb", "lobs": 70, "split_by_role": True})
        pl = plan.plan(f)
        g = [(x["class"], x["mode"], x["table_names"], x["tables"]) for x in pl["modules"][0]["groups"]]
        self.assertEqual(g, [("facts", "continuous", ["history"], 70),
                             ("dimensions", "triggered", ["branch", "teller", "account"], 210)])   # 250 // 3 = 83 LOBs
        r = plan.resources(pl["modules"][0], "https://example.invalid/a.zip")["resources"]
        facts, dims = r["pipelines"]["bank_ingest_1"], r["pipelines"]["bank_ingest_2"]
        self.assertTrue(facts["continuous"])
        self.assertFalse(dims["continuous"])
        self.assertEqual(facts["ingestion_definition"]["objects"][0]["table"]["source_table"], "history")
        self.assertEqual([t["task_key"] for t in r["jobs"]["bank_refresh"]["tasks"]], ["ingest_2", "merge", "transform"])
        facts_job = r["jobs"]["bank_facts"]["tasks"][0]["notebook_task"]["base_parameters"]
        self.assertEqual(facts_job["selection"].split(","), [   # qualified: gold's views are in the gold schema
            "${var.catalog}.${resources.schemas.bank_silver.name}.all_history",
            "${var.catalog}.${resources.schemas.bank_silver.name}.silver_transactions",
            "${var.catalog}.${resources.schemas.bank_gold.name}.transactions",
            "${var.catalog}.${resources.schemas.bank_gold.name}.unusual_by_hour",
            "${var.catalog}.${resources.schemas.bank_gold.name}.lob_summary"])
        self.assertEqual(timings.scale(pl, "bank")["group_tables"], [70, 210])   # the plan's own pipelines
        self.assertEqual(timings.scale(pl, "bank", lobs=100)["group_tables"], [100, 249, 51])   # per class
        mixed = plan.plan(project(self.d, bank={"pack": "tpcb", "lobs": 70}))["modules"][0]["groups"]
        self.assertEqual([(x["class"], x["table_names"]) for x in mixed], [("all", None), ("all", None)])   # the default

    def test_limits_refused(self):
        f = project(self.d, bank={"pack": "tpcb", "lobs": 3})
        p = yaml.safe_load(open(f))
        p["limits"] = {"tables_per_pipeline": 1000}
        yaml.safe_dump(p, open(f, "w"))
        with self.assertRaisesRegex(ValueError, "at most 999"):
            plan.plan(f)
        p["limits"], p["source"]["max_slots"] = {}, 1
        p["modules"]["bank"]["lobs"] = 100
        yaml.safe_dump(p, open(f, "w"))
        with self.assertRaisesRegex(ValueError, "replication slots"):
            plan.plan(f)

    def test_render_writes_resources_marts_and_plan(self):
        f = project(self.d, bank={"pack": "tpcb", "lobs": 70, "data": True}, scale={"pack": "tpcb", "lobs": 3})
        got = plan.render(f, "https://example.invalid/a.zip")
        self.assertEqual(got["modules"]["bank"], {"lobs": 70, "pipelines": 2, "tables": 280})
        r = yaml.safe_load(open(os.path.join(self.d, "resources", "bank.yml")))["resources"]
        self.assertEqual(len([s for s in r["schemas"] if "_bronze_" in s]), 70)
        ing = r["pipelines"]["bank_ingest_2"]["ingestion_definition"]
        self.assertEqual(len(ing["objects"]), 8)
        self.assertEqual(ing["source_configurations"][0]["catalog"]["postgres"]["slot_config"],
                         {"slot_name": "p_bank_2", "publication_name": "p_bank_2_pub"})
        t = r["pipelines"]["bank_transform"]
        self.assertEqual(t["configuration"]["large_withdrawal"], "5000")
        self.assertEqual([x["file"]["path"] for x in t["libraries"]],
                         ["../transform/n1.py", "../marts/tpcb/silver.sql", "../marts/tpcb/gold.sql"])
        self.assertTrue(os.path.isfile(os.path.join(self.d, "marts", "tpcb", "gold.sql")))
        job = r["jobs"]["bank_refresh"]
        keys = [t["task_key"] for t in job["tasks"]]
        self.assertEqual(keys, ["ingest_1", "ingest_2", "merge", "transform"])
        self.assertEqual([t["task_key"] for t in r["jobs"]["bank_verify"]["tasks"]], ["verify"])   # off the path
        merge = job["tasks"][2]
        self.assertEqual(merge["python_wheel_task"]["entry_point"], "lfc-medallion-merge")
        # the merge route takes the same roles the pipelines are split by: the schema YAML's
        self.assertEqual(json.loads(merge["python_wheel_task"]["named_parameters"]["hints"]),
                         {"account": "dimension", "branch": "dimension", "history": "log", "teller": "dimension"})
        self.assertEqual(t["configuration"]["silver_schema"], "${resources.schemas.bank_silver.name}")
        open(os.path.join(self.d, "marts", "tpcb", "gold.sql"), "a").write("-- mine\n")
        plan.render(f, "https://example.invalid/a.zip")
        self.assertTrue(open(os.path.join(self.d, "marts", "tpcb", "gold.sql")).read().endswith("-- mine\n"))
        pl = plan.read_plan(os.path.join(self.d, "source", "plan.json"))
        self.assertEqual([m["module"] for m in pl["modules"]], ["bank", "scale"])

    def test_init_fills_the_templates_once(self):
        got = cli.init(self.d, "acme", "https://example.invalid/a.zip")
        self.assertIn(os.path.join(self.d, "acme.yaml"), got)
        src = open(os.path.join(self.d, "source", "databricks.yml")).read()
        self.assertIn("acme_source", src)
        self.assertNotIn("@@", src)
        self.assertEqual(cli.init(self.d, "acme", "x"), [])


class Check(unittest.TestCase):
    def test_tpcb_pack_is_right(self):
        self.assertEqual(check.check(pack.load("tpcb"), COLS), [])

    def test_mistakes_found(self):
        d = tempfile.mkdtemp()
        try:
            shutil.copytree(pack.load("tpcb")["dir"], os.path.join(d, "p"))
            with open(os.path.join(d, "p", "silver.sql"), "a") as fh:
                fh.write("\nCREATE OR REFRESH MATERIALIZED VIEW bad AS SELECT h.h_amount, t.t_id FROM all_history h "
                         "JOIN all_teller t ON t.t_id = h.h_t_id JOIN all_nation n ON n.lob = h.lob;\n")
            got = check.check(pack.load(os.path.join(d, "p")), COLS)
        finally:
            shutil.rmtree(d)
        self.assertTrue(any("h_amount" in p for p in got))
        self.assertTrue(any("join of t doesn't equate lob" in p for p in got))
        self.assertTrue(any("no table nation" in p for p in got))


class Verify(unittest.TestCase):
    def test_mart_names_and_bind(self):
        sql = open(os.path.join(pack.load("tpcb")["dir"], "gold.sql")).read()
        self.assertEqual(verify.mart_names(sql), ["transactions", "unusual_by_hour", "lob_summary"])
        self.assertEqual(verify.bind("(:base + :teller) / 1000.0", {"base": 5, "teller": 60}), "(5 + 60) / 1000.0")


class Checksum(unittest.TestCase):
    def test_an_instant_and_a_wall_time(self):   # timestamptz: its own epoch, not through the session's time zone
        from lfc_medallion_pack import checksum
        pg = checksum.row_text([{"name": "a", "type": "timestamptz"}, {"name": "b", "type": "timestamp"}], "postgres")
        self.assertIn('EXTRACT(EPOCH FROM ("a")::timestamptz)', pg)
        self.assertIn('EXTRACT(EPOCH FROM ("b")::timestamp)', pg)


class Dashboard(unittest.TestCase):
    def test_every_widget_binds_its_fields_and_rows_fill_the_grid(self):
        d = dashboard.build("p")
        names = {x["name"] for x in d["datasets"]}
        for page in d["pages"]:
            self.assertEqual(page["layoutVersion"], "GRID_V1")
            rows: dict[int, int] = {}
            for item in page["layout"]:
                w, pos = item["widget"], item["position"]
                rows[pos["y"]] = rows.get(pos["y"], 0) + pos["width"]
                for q in w.get("queries", []):
                    self.assertIn(q["query"]["datasetName"], names)
                    fields = {f["name"] for f in q["query"]["fields"]}
                    enc = w["spec"]["encodings"]
                    bound = [c["fieldName"] for c in enc.get("columns", enc.get("fields", []))] + \
                        [v["fieldName"] for k, v in enc.items() if isinstance(v, dict) and "fieldName" in v] + \
                        [f["fieldName"] for v in enc.values() if isinstance(v, dict) for f in v.get("fields", [])]
                    self.assertTrue(set(bound) <= fields, (w["name"], set(bound) - fields))
            self.assertTrue(all(v == 12 for v in rows.values()), (page["name"], rows))


class Timings(unittest.TestCase):
    def test_scale_at_a_past_size(self):
        d = tempfile.mkdtemp()
        try:
            pl = plan.plan(project(d, bank={"pack": "tpcb", "lobs": 250}))
        finally:
            shutil.rmtree(d)
        sc = timings.scale(pl, "bank", lobs=3)
        self.assertEqual((sc["lobs"], sc["tables"], sc["pipelines"], sc["group_tables"]), (3, 12, 1, [12]))
        self.assertEqual(timings.scale(pl, "bank")["group_tables"], [248, 248, 248, 248, 8])

    def test_predict_fits_two_sizes_and_keeps_one_as_is(self):
        base = {"date": "d", "module": "m", "data": False, "lobs": 1, "pipelines": 1, "tables_per_pipeline": 1}
        rows = [base | {"step": "ingest_1", "kind": "steady", "tables": 1000, "pipeline_tables": 8, "seconds": 60},
                base | {"step": "ingest_2", "kind": "steady", "tables": 1000, "pipeline_tables": 248, "seconds": 680},
                base | {"step": "ingest_3", "kind": "first", "tables": 1000, "pipeline_tables": 248, "seconds": 800,
                        "result": "FAILED"},
                base | {"step": "verify", "tables": 40, "seconds": 70, "data": True}]
        got = timings.predict(rows, 488)
        self.assertEqual(got["ingest steady"]["seconds"], 1300.0)          # 60 + (680 - 60) / 240 * 480
        self.assertEqual(got["verify data"], {"seconds": 70.0, "fit": "none", "from_tables": [40]})
        self.assertNotIn("ingest first", got)                              # a failed task isn't a time
        self.assertIn("| ingest_2, its 248 |", timings.report(rows))


if __name__ == "__main__":
    unittest.main()
