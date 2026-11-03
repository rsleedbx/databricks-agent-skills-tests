"""Offline checks of pipeline-diagram.py on saved pipeline, gateway and connection JSON (names are placeholders)."""
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "scripts", "pipeline-diagram.py")


# the files the script reads from this machine by default (~/.config/lfc) point at nothing, so the tests don't depend on them
HERMETIC = {**os.environ, "LFC_SAAS_TABLES": os.devnull + ".none", "LFC_FACTS_DIR": os.devnull + ".none"}


def run(*args):
    return subprocess.run([sys.executable, SCRIPT, *args], check=True, capture_output=True, text=True, env=HERMETIC).stdout


def f(name):
    return os.path.join(HERE, name)


class Gateway(unittest.TestCase):
    def setUp(self):
        self.out = run("--pipeline-json", f("gateway-pipeline.json"), "--gateway-json", f("gateway-gateway.json"),
                       "--connection-json", f("gateway-connection.json"),
                       "--workspace-host", "https://adb-1.2.azuredatabricks.net", "--source-cloud", "azure",
                       "--source-host", "VM + Podman", "--primary", "203.0.113.10,30005",
                       "--secondary", "203.0.113.10,30006", "--firewall", "NSG", "--secret-kind", "uc")

    def test_text(self):
        self.assertIn("[primary]   [secondary*]", self.out)
        self.assertIn("gateway demo_ag_gw (classic cluster, continuous)", self.out)
        self.assertIn("staging: main.demo_ag_stg", self.out)
        self.assertIn("<-- UC secret", self.out)
        self.assertIn("Databricks (Azure)", self.out)

    def test_mermaid(self):
        self.assertIn("flowchart TD", self.out)
        self.assertIn("N0 -. availability group .-> N1", self.out)
        self.assertIn("N1 --> CAP", self.out)  # the connection points at the secondary: capture read there
        self.assertIn("CAP --- FW --- C", self.out)
        self.assertIn("change capture: CDC or change tracking, read on the secondary", self.out)
        self.assertIn("G --> STG --> A", self.out)


class Integrated(unittest.TestCase):
    def test_integrated(self):
        out = run("--pipeline-json", f("integrated-pipeline.json"), "--connection-json", f("integrated-connection.json"),
                  "--workspace-host", "https://x.cloud.databricks.com", "--hide-host")
        self.assertIn("(integrated CDC, serverless): extract -> staging -> apply", out)
        self.assertIn("streaming tables (SCD 2)", out)
        self.assertIn("Databricks (AWS)", out)
        self.assertNotIn("1433", out)  # --hide-host
        self.assertIn("X[extract] --> STG[(staging volume)] --> APL[apply]", out)


class Facts(unittest.TestCase):
    def test_vm_container_juicefs(self):
        out = run("--pipeline-json", f("integrated-pipeline.json"), "--connection-json", f("integrated-connection.json"),
                  "--source-facts", f("vm-facts.json"))
        self.assertIn("VM Standard_D16s_v5: 16 vCPU, 64 GB RAM; disks: os 64 GB Premium_LRS, data 256 GB PremiumV2_LRS (3000 IOPS, 125 MB/s)", out)
        self.assertIn("container demo-db-s: CPU not limited, RAM not limited (SQL Server max memory 4 GB), data 2.8 GB on the disk", out)
        self.assertIn("JuiceFS demo-jfs at /mnt/jfs (object storage: Azure Blob)", out)
        self.assertIn('HW["VM Standard_D16s_v5<br/>16 vCPU, 64 GB RAM<br/>disk 256 GB PremiumV2_LRS, 3000 IOPS', out)

    def test_facts_found_by_connection_name(self):
        import json
        import shutil
        import tempfile
        d = tempfile.mkdtemp()
        try:
            facts = json.load(open(f("vm-facts.json"))); facts["source_host"] = "VM + Podman"
            json.dump(facts, open(os.path.join(d, "my_sqlserver_conn.json"), "w"))
            out = subprocess.run([sys.executable, SCRIPT, "--pipeline-json", f("integrated-pipeline.json"),
                                  "--connection-json", f("integrated-connection.json"), "--format", "text"],
                                 check=True, capture_output=True, text=True, env={**HERMETIC, "LFC_FACTS_DIR": d}).stdout
        finally:
            shutil.rmtree(d)
        self.assertIn("Azure: VM + Podman, SQL Server", out)   # cloud and host from the facts
        self.assertIn("VM Standard_D16s_v5: 16 vCPU", out)

    def test_vm_text(self):
        out = run("--pipeline-json", f("integrated-pipeline.json"), "--connection-json", f("integrated-connection.json"),
                  "--vm", "Standard_D2s_v5, 2 vCPU, 8 GB RAM, 64 GB Premium SSD", "--format", "text")
        self.assertIn("VM Standard_D2s_v5, 2 vCPU, 8 GB RAM, 64 GB Premium SSD", out)


class Model(unittest.TestCase):
    """The data model drawn above the source: statschema models, mautic, an override, the facts' workload."""

    def out(self, *extra, fmt="both"):
        return run("--pipeline-json", f("integrated-pipeline.json"), "--connection-json", f("integrated-connection.json"),
                   "--format", fmt, *extra)

    def test_statschema_model(self):
        out = self.out("--model", "tpcc", "--model-scale", "SF 1")
        self.assertTrue(out.startswith("model tpcc: 9 tables, SF 1 (statschema + datagen)\n        | loads the source"))
        self.assertIn('WL[/"model tpcc<br/>9 tables, SF 1<br/>statschema + datagen"/]', out)
        self.assertIn("  WL --> N0", out)

    def test_mautic_and_override(self):
        self.assertIn("model mautic: 128 tables (the application: its own install and traffic)", self.out("--model", "mautic", fmt="text"))
        self.assertIn("model tpch: 3 tables", self.out("--model", "tpch", "--model-tables", "3", fmt="text"))
        self.assertIn("model orders_app: tables: ? (the test's own tables)", self.out("--model", "orders_app", fmt="text"))

    def test_from_facts_and_absent(self):
        import json
        import tempfile
        facts = json.load(open(f("vm-facts.json"))); facts["workload"] = {"model": "tpcds", "scale": "SF 0.1"}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as t:
            json.dump(facts, t)
        try:
            self.assertIn("model tpcds: 24 tables, SF 0.1", self.out("--source-facts", t.name, fmt="text"))
        finally:
            os.unlink(t.name)
        self.assertNotIn("WL", self.out(fmt="mermaid"))   # no model: no box


class Capture(unittest.TestCase):
    """The change-capture line per engine, from a connection type (and events or spec where it depends on them)."""

    def variant(self, ctype, pipeline_edit=None, extra=()):
        import json
        import tempfile
        conn = json.load(open(f("integrated-connection.json"))); conn["connection_type"] = ctype
        ap = json.load(open(f("integrated-pipeline.json")))
        if pipeline_edit:
            pipeline_edit(ap["spec"]["ingestion_definition"])
        with tempfile.TemporaryDirectory() as d:
            cj, aj = os.path.join(d, "c.json"), os.path.join(d, "a.json")
            json.dump(conn, open(cj, "w")); json.dump(ap, open(aj, "w"))
            return run("--pipeline-json", aj, "--connection-json", cj, "--format", "text", *extra)

    def test_sqlserver_events(self):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as e:
            json.dump({"events": [{"message": "Replication tables: 2 CT tables, 3 CDC tables"}]}, e)
        try:
            self.assertIn("change capture: CDC + change tracking", self.variant("SQLSERVER", extra=("--events-json", e.name)))
        finally:
            os.unlink(e.name)

    def test_sqlserver_flag(self):
        self.assertIn("change capture: change tracking", self.variant("SQLSERVER", extra=("--capture", "ct")))

    def test_engines(self):
        self.assertIn("change capture: LogMiner", self.variant("ORACLE"))
        self.assertIn("change capture: binlog", self.variant("MYSQL"))
        pg = self.variant("POSTGRESQL", lambda d: d.update(source_configurations=[{"catalog": {
            "source_catalog": "db", "postgres": {"slot_config": {"slot_name": "s1", "publication_name": "p1"}}}}]))
        self.assertIn("slot s1, publication p1", pg)

    def test_query_based_and_saas(self):
        self.assertIn("change capture: query-based", self.variant("SQLSERVER", lambda d: d.update(connector_type="QUERY_BASED")))
        saas = self.variant("SALESFORCE")
        self.assertIn("SaaS: Salesforce REST API", saas)
        self.assertIn("API reads -> apply", saas)


class Saas(unittest.TestCase):
    """SaaS: the REST host from the connection's options or the connector's public API host; the rest is shared."""

    def draw(self, ctype, options, *extra):
        import json
        import tempfile
        conn = {"name": "my_saas_conn", "connection_type": ctype, "options": options}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as c:
            json.dump(conn, c)
        try:
            return run("--pipeline-json", f("integrated-pipeline.json"), "--connection-json", c.name, *extra)
        finally:
            os.unlink(c.name)

    def test_host_from_options(self):
        out = self.draw("SALESFORCE", {"instance_url": "https://example.my.salesforce.com", "client_id": "x"})
        self.assertIn("SaaS: Salesforce REST API at example.my.salesforce.com", out)
        self.assertIn('N0["Salesforce REST API<br/>example.my.salesforce.com"]', out)
        self.assertIn("change capture: API reads:", out)
        self.assertNotIn("[database*]", out)
        self.assertNotIn("-> db.example.com", out)

    def test_public_api_host(self):
        self.assertIn("SaaS: Google Ads REST API at googleads.googleapis.com", self.draw("GOOGLE_ADS", {"client_id": "x"}, "--format", "text"))

    def test_connector_tables_file(self):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as t:
            json.dump({"version": 1, "connectors": {"JIRA": {"tables": 29, "incremental": 6, "full": 23, "inventory": "finite"},
                                                    "SALESFORCE": {"inventory": "runtime"}}}, t)
        env = {**HERMETIC, "LFC_SAAS_TABLES": t.name}
        try:
            for ctype, want in (("JIRA", "connector tables: 29 tables: 6 incremental, 23 full refresh"),
                                ("SALESFORCE", "connector tables: objects discovered from the source")):
                import json as j
                with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as c:
                    j.dump({"name": "c", "connection_type": ctype, "options": {}}, c)
                out = subprocess.run([sys.executable, SCRIPT, "--pipeline-json", f("integrated-pipeline.json"),
                                      "--connection-json", c.name, "--format", "text"],
                                     check=True, capture_output=True, text=True, env=env).stdout
                os.unlink(c.name)
                self.assertIn(want, out)
        finally:
            os.unlink(t.name)

    def test_managed_host_names_the_service_flags_override(self):
        conn = {"name": "c", "connection_type": "SQLSERVER", "options": {"host": "srv.database.windows.net", "port": "1433"}}
        import json, tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as c:
            json.dump(conn, c)
        try:
            base = ("--pipeline-json", f("integrated-pipeline.json"), "--connection-json", c.name, "--format", "text")
            self.assertTrue(run(*base).startswith("Azure: Azure SQL Database, SQL Server"))
            self.assertTrue(run(*base, "--source-cloud", "aws", "--source-host", "X").startswith("AWS: X, SQL Server"))
        finally:
            os.unlink(c.name)

    def test_mermaid_keeps_placeholders(self):
        out = Capture().variant("POSTGRESQL", extra=("--format", "mermaid"))   # no slot in the spec
        self.assertIn("slot &lt;slot&gt; / publication &lt;publication&gt;", out)
        self.assertIn("<br/>", out)

    def test_load_tool_and_medallion(self):
        out = self.draw("JIRA", {}, "--model", "jira-test-data-generator", "--model-scale", "Small, 5000 issues",
                        "--model-tables", "12", "--reverse-etl", "PostgreSQL:synced tables:2", "--format", "text")
        self.assertTrue(out.startswith("model jira-test-data-generator: 12 tables, Small, 5000 issues"))
        self.assertIn("SaaS: Jira REST API at <site>.atlassian.net", out)
        self.assertIn("    L1 landing", out)
        self.assertIn("reverse ETL -> PostgreSQL (synced tables): 2 tables", out)


class Kafka(unittest.TestCase):
    def test_created(self):
        out = run("--pipeline-json", f("kafka-pipeline.json"), "--connection-json", f("kafka-connection.json"),
                  "--workspace-host", "https://adb-1.2.azuredatabricks.net", "--broker", "Standard, 4-40 TU",
                  "--producer", "producer.py", "--producer-host", "Laptop", "--probe", "freshness probe")
        self.assertIn("Azure: Event Hubs (Kafka endpoint) at demo-ns.servicebus.windows.net:9093", out)
        self.assertIn("topic demo-topic", out)
        self.assertIn("connection demo_kafka (KAFKA, SASL PLAIN, user $ConnectionString)", out)
        self.assertIn("(serverless, continuous, channel PREVIEW): Kafka consumer from offset latest", out)
        self.assertIn("main.demo_kafka.events: key STRING, value VARIANT (JSON)", out)
        self.assertNotIn("planned", out)
        self.assertNotIn("SaaS", out)   # a KAFKA connection isn't drawn as a REST API
        self.assertIn('PROD -->|"Kafka produce, SASL PLAIN"| TOP', out)
        self.assertIn("TOP --- C", out)
        self.assertIn("T -.-> PRB", out)

    def test_planned_from_the_spec(self):
        out = run("--pipeline-json", f("kafka-spec.json"), "--connection-json", f("kafka-connection.json"),
                  "--format", "text", "--hide-host")
        self.assertIn("[planned: not created yet]", out)
        self.assertNotIn("servicebus", out)   # --hide-host


if __name__ == "__main__":
    unittest.main()
