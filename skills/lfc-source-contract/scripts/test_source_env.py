"""Offline checks of the source contract (contract 1): source_env.py on every fixture (valid, missing field, bad port or
engine, contract 2), each engine's default client (fake clients on PATH record their arguments and environment: the
password is never an argument), and a Kafka broker's contract. Replaces test-source-env.sh.

  python3 -I skills/lfc-source-contract/scripts/test_source_env.py
"""
from __future__ import annotations

import os
import shlex
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
FX = HERE / "fixtures"
SE = HERE / "lfc_source_contract" / "source_env.py"
SQL_CLIENT = HERE / "lfc_source_contract" / "sql_client.py"
sys.path.insert(0, str(HERE))

FAKE = f"""#!{sys.executable}
# fake client for test_source_env.py: logs its arguments, the password variables and its stdin
import os, sys
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write("ARGV " + os.path.basename(sys.argv[0]) + " " + " ".join(sys.argv[1:]) + "\\n")
    log.write("ENV " + " ".join(f"{{k}}={{os.environ[k]}}" for k in ("SQLCMDPASSWORD", "PGPASSWORD", "MYSQL_PWD")
                               if k in os.environ) + "\\n")
    log.write("STDIN\\n" + sys.stdin.read())
"""


def run_se(*args: str, env=None):
    return subprocess.run([sys.executable, "-I", str(SE), *args], capture_output=True, text=True, env=env, check=False)


def exports_of(name: str) -> dict:
    out = run_se(str(FX / f"{name}.json")).stdout
    return {line.split("=", 1)[0].replace("export ", ""): shlex.split(line.split("=", 1)[1])[0]
            for line in out.splitlines() if line.startswith("export ")}


def json_field(fixture: str, key: str) -> str:
    import json
    return str(json.loads((FX / f"{fixture}.json").read_text())[key])


class SourceEnv(unittest.TestCase):
    def test_usage(self):
        r = run_se()
        self.assertEqual(r.returncode, 2)
        self.assertIn("source_env.py <source.json>", r.stdout)

    def test_valid_fixtures_exit_0(self):
        for e in ["sqlserver", "postgres", "mysql", "mariadb", "oracle", "db2", "informix", "cockroachdb",
                  "yugabytedb", "sql-cmd-and-connection"]:
            with self.subTest(engine=e):
                self.assertEqual(run_se(str(FX / f"{e}.json")).returncode, 0)

    def test_password_not_exported(self):
        self.assertNotIn(json_field("sqlserver", "password"), run_se(str(FX / "sqlserver.json")).stdout)

    def test_round_trip(self):
        e = exports_of("postgres")
        self.assertEqual((e["SRC_HOST"], e["SRC_PORT"], e["CONN_USER"], e["SRC_DB"], e["SRC_ENGINE"]),
                         ("203.0.113.10", "5432", "lfc_user", "lfc", "postgres"))

    def test_oracle_service(self):
        self.assertEqual(exports_of("oracle")["SRC_DB"], "FREEPDB1")

    def test_connection_facts_and_sql_cmd(self):
        e = exports_of("sql-cmd-and-connection")
        self.assertTrue(e["SRC_SQL"].endswith("/usr/bin/true"))
        self.assertEqual(e["CONNECTION"], "me_pg1")
        self.assertEqual(e["SRC_FACTS"], str(FX / "facts.json"))

    def test_no_connection_leaves_none_exported(self):
        self.assertNotIn("CONNECTION", exports_of("postgres"))

    def test_db2_has_no_default_client(self):
        self.assertNotIn("SRC_SQL", exports_of("db2"))
        r = run_se(str(FX / "informix.json"))
        self.assertIn("no default SQL client", r.stderr)

    def test_insecure_default_client(self):
        self.assertEqual(exports_of("cockroachdb")["SRC_SQL"], str(SQL_CLIENT))

    def test_kafka_broker(self):
        e = exports_of("kafka")
        r = run_se(str(FX / "kafka.json"))
        self.assertEqual((e["SRC_ENGINE"], e["SRC_HOST"], e["SRC_PORT"], e["CONN_USER"], e["SRC_TOPIC"], e["SRC_SASL"]),
                         ("kafka", "demo-ns.servicebus.windows.net", "9093", "$ConnectionString",
                          "tag-observations", "PLAIN"))
        self.assertEqual(e["SRC_PASSWORD_FILE"], str(FX / "demo-ns.pw"))
        self.assertNotIn("SRC_SQL", e)
        self.assertEqual(r.stderr, "")

    def refused(self, fixture: str, *words: str):
        r = run_se(str(FX / f"{fixture}.json"))
        self.assertEqual((r.returncode, r.stdout), (2, ""))
        for w in words:
            self.assertIn(w, r.stderr)

    def test_missing_host(self):
        self.refused("missing-host", "missing host")

    def test_kafka_without_topic(self):
        self.refused("kafka-no-topic", "missing topic")

    def test_oracle_without_service(self):
        self.refused("missing-service", "missing service")

    def test_port_as_text(self):
        self.refused("bad-port", "port must be a whole number")

    def test_unknown_engine(self):
        self.refused("bad-engine", "is not one of")

    def test_contract_2(self):
        self.refused("contract-2", "contract 2")

    def test_no_contract_field(self):
        self.refused("no-contract", "missing contract")

    def test_not_json(self):
        with tempfile.TemporaryDirectory() as t:
            bad = Path(t) / "bad.json"
            bad.write_text("not json\n")
            self.assertNotEqual(run_se(str(bad)).returncode, 0)


class DefaultClients(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fb = Path(self.tmp.name) / "bin"
        self.fb.mkdir()
        for c in ["sqlcmd", "psql", "mysql", "mariadb", "sqlplus"]:
            p = self.fb / c
            p.write_text(FAKE)
            p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        self.log = Path(self.tmp.name) / "log"

    def tearDown(self):
        self.tmp.cleanup()

    def client(self, engine: str, *args: str):
        env = dict(os.environ, PATH=f"{self.fb}{os.pathsep}{os.environ['PATH']}", FAKE_LOG=str(self.log),
                   SRC_CONTRACT=str(FX / f"{engine}.json"))
        self.log.write_text("")
        return subprocess.run([sys.executable, "-I", str(SQL_CLIENT), *args], input="SELECT 1;\n",
                              capture_output=True, text=True, env=env, check=False)

    def test_each_engine_runs_its_client(self):
        cases = {"sqlserver": "sqlcmd", "postgres": "psql", "cockroachdb": "psql", "yugabytedb": "psql",
                 "mysql": "mysql", "mariadb": "mariadb", "oracle": "sqlplus"}
        for engine, client in cases.items():
            with self.subTest(engine=engine):
                self.client(engine, "mydb")
                text = self.log.read_text()
                self.assertIn(f"ARGV {client} ", text)
                self.assertIn("SELECT 1", text)
                pw = json_field_dict(engine).get("password", "")
                if pw:
                    argv_lines = [line for line in text.splitlines() if line.startswith("ARGV")]
                    self.assertFalse(any(pw in line for line in argv_lines))
                    if client != "sqlplus":
                        self.assertIn(pw, text)

    def test_sqlserver_arguments(self):
        self.client("sqlserver", "mydb")
        self.assertIn("ARGV sqlcmd ", self.log.read_text())
        self.assertRegex(self.log.read_text(), r"-S 203\.0\.113\.10,1433 .*-d mydb")

    def test_postgres_arguments(self):
        self.client("postgres", "mydb")
        self.assertRegex(self.log.read_text(), r"ARGV psql .*-p 5432 .*-d mydb")

    def test_arguments_after_the_database_go_last(self):
        self.client("postgres", "mydb", "-F", "|")
        self.assertRegex(self.log.read_text(), r"ARGV psql .*-d mydb .*-F \t -F \|")

    def test_oracle_password_in_the_sql(self):
        self.client("oracle", "mydb")
        self.assertIn("CONNECT lfc_user/", self.log.read_text())


def json_field_dict(fixture: str) -> dict:
    import json
    return json.loads((FX / f"{fixture}.json").read_text())


if __name__ == "__main__":
    unittest.main()
