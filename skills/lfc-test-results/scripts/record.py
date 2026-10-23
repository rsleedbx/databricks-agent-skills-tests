#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""record.py: validate test-result records and append them to results/runs/<run>.jsonl.

  uv run record.py < records.jsonl            # one JSON object per line (or one JSON array)
  uv run record.py --check                    # re-validate every recorded result

Each run has its own file, so people recording in parallel never edit the same file. Fills `skills` with
both repos' current commits when it's missing. Refuses the whole batch if any record is invalid or looks
like it holds something internal (the repo is public).
"""
import json, re, subprocess, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]                 # skills/lfc-test-results/scripts/record.py
RUNS = REPO / "results" / "runs"                         # one <run>.jsonl per run
SKILLS_REPO = REPO.parent / "databricks-agent-skills"

REQUIRED = {"date", "run", "test", "subject", "outcome", "result"}
OUTCOMES = {"pass", "fail", "blocked", "expected-stop", "partial"}
TESTS = {"connection", "qbc", "icdc", "scdc", "fcqbc", "dml", "source-setup",      # ingestion and setup
         "secret-by-reference", "network", "compat",                                  # credentials, network path, versions
         "rclone-pull", "reflink-isolation", "juicefs-clone", "disk-restore",      # branch layers
         "snapshot-read", "write-tps", "load",                                      # performance
         "saas", "other"}                                                            # SaaS connector pipelines; add new names here in the same PR
ALLOWED_KEYS = REQUIRED | {"host", "workspace", "setup", "compute", "metrics", "error", "skills", "docs", "notes"}
# compute: what the run set (serverless), what the latest update ran on (ran_on, from pipelines get-update
# .update.config.serverless), whether that compute came up (started) and then connected to the source
# (reached_source), and if not, why (blocked_by). A rerun that changed the setting says from what and why
# (switched_from, switch_reason). gateway: the same for a standard CDC gateway. bin/compute_report.sh reads them.
SERVERLESS = {"unset", "true", "false"}
RAN_ON = {"serverless", "classic", "not-started", "unknown"}   # unknown: not read back
BLOCKED_BY = {"compute-not-allowed", "firewall", "feature-gate", "other"}
PIPELINE_TESTS = {"qbc", "fcqbc", "icdc", "scdc", "dml", "saas"}
SETUP_KEYS = {"storage", "network", "credentials", "workload", "dml", "tool", "contributor"}   # documented; others allowed (warned)

# Anything matching these is internal or secret; use placeholders (see SKILL.md)
LEAKS = [
    (r"(?<![\d.])(?!(?:192\.0\.2|198\.51\.100|203\.0\.113)\.)(?:\d{1,3}\.){3}\d{1,3}(?![\d.]|/)", "an IP address (use 203.0.113.x)"),
    (r"[\w.+-]+@[\w-]+\.[\w.]+", "an email address"),
    (r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", "a UUID (pipeline, update or subscription ID)"),
    (r"\b\d{15,16}\b", "a workspace or account ID"),
    (r"adb-\d+|\.azuredatabricks\.net|\.cloud\.databricks\.com|\.gcp\.databricks\.com", "a workspace URL"),
    (r"\.(?:blob|dfs)\.core\.windows\.net|\.amazonaws\.com|\.database\.azure\.com", "a cloud resource hostname"),
    (r"\bsig=|SharedAccessSignature|dapi[0-9a-f]{20,}|AKIA[0-9A-Z]{16}", "a credential"),
    (r"Lfc1[0-9a-f]{12,}", "a generated password"),
]

VERSION = r"\d+\.\d+\.\d+\.\d+(?![\d.])"   # 11.5.8.0: a four-part engine version, not an address
def leaks(rec):
    # subject.version and subject.image, and the tag of an image reference anywhere (".../db2:11.5.8.0": a name with a
    # "/" before the ":"), hold four-part versions such as Db2's, which the address pattern would flag. Nothing else is
    # masked: "tcp:10.1.2.3" or "host:10.0.0.4" is still an address.
    r = json.loads(json.dumps(rec))
    for k in ("version", "image"):
        if isinstance(r.get("subject"), dict) and isinstance(r["subject"].get(k), str):
            r["subject"][k] = re.sub(r"(?<![\d.])" + VERSION, "<v>", r["subject"][k])
    text = re.sub(r"((?:[\w.-]+/)+[\w.-]+:)" + VERSION, r"\1<v>", json.dumps(r))
    return [why for pat, why in LEAKS if re.search(pat, text)]

def problems(rec):
    out = []
    if not isinstance(rec, dict): return ["not a JSON object"]
    missing = REQUIRED - rec.keys()
    if missing: out.append(f"missing {sorted(missing)}")
    extra = rec.keys() - ALLOWED_KEYS
    if extra: out.append(f"unknown keys {sorted(extra)}")
    if rec.get("outcome") not in OUTCOMES: out.append(f"outcome must be one of {sorted(OUTCOMES)}")
    if rec.get("test") not in TESTS: out.append(f"test must be one of {sorted(TESTS)}")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(rec.get("date", ""))): out.append("date must be YYYY-MM-DD")
    if not isinstance(rec.get("subject"), dict) or "engine" not in rec.get("subject", {}): out.append("subject needs an engine")
    if rec.get("outcome") != "pass" and not rec.get("error") and not rec.get("notes"): out.append("a non-pass needs error or notes")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}-[a-z0-9-]+", str(rec.get("run", ""))): out.append("run must be <YYYY-MM-DD>-<topic> in lowercase kebab case")
    for k in ("workspace", "setup", "compute", "metrics"):
        if k in rec and not isinstance(rec[k], dict): out.append(f"{k} must be an object")
    c = rec.get("compute")
    if isinstance(c, dict):
        for name, part in (("compute", c), ("compute.gateway", c.get("gateway"))):
            if part is None: continue
            if not isinstance(part, dict): out.append(f"{name} must be an object"); continue
            if part.get("serverless") not in SERVERLESS: out.append(f"{name}.serverless must be one of {sorted(SERVERLESS)}")
            if part.get("ran_on") not in RAN_ON: out.append(f"{name}.ran_on must be one of {sorted(RAN_ON)}")
            for k in ("started", "reached_source"):
                if k in part and not isinstance(part[k], bool): out.append(f"{name}.{k} must be true or false")
            if "blocked_by" in part and part["blocked_by"] not in BLOCKED_BY: out.append(f"{name}.blocked_by must be one of {sorted(BLOCKED_BY)}")
            if (part.get("started") is False or part.get("reached_source") is False) and "blocked_by" not in part:
                out.append(f"{name}: not started or source not reached needs blocked_by")
            if "switched_from" in part and (part["switched_from"] not in SERVERLESS or not part.get("switch_reason")):
                out.append(f"{name}.switched_from must be one of {sorted(SERVERLESS)}, with a switch_reason")
    return out + [f"looks internal: {w}" for w in leaks(rec)]

def head(path):
    try: return subprocess.run(["git", "-C", str(path), "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    except Exception: return None

def load_all():
    files = sorted(RUNS.glob("*.jsonl")) if RUNS.exists() else []
    return [(f, r) for f in files for r in load(f.read_text())]

def load(text):
    text = text.strip()
    if not text: return []
    return json.loads(text) if text.startswith("[") else [json.loads(l) for l in text.splitlines() if l.strip()]

def main():
    if "--check" in sys.argv:
        pairs = load_all(); recs = [r for _, r in pairs]
        wrong = [f.name for f, r in pairs if isinstance(r, dict) and f.stem != r.get("run")]
        if wrong: print(f"records in the wrong file (file name must be the run): {sorted(set(wrong))}", file=sys.stderr); sys.exit(1)
    else:
        recs = load(sys.stdin.read())
        auto = {k: v for k, v in {"databricks-agent-skills": head(SKILLS_REPO), "databricks-agent-skills-tests": head(REPO)}.items() if v}
        for r in recs:
            if isinstance(r, dict) and "skills" not in r and auto: r["skills"] = auto
    bad = [(i, p) for i, r in enumerate(recs) for p in problems(r)]
    for i, r in enumerate(recs):
        extra = set(r.get("setup", {})) - SETUP_KEYS if isinstance(r, dict) and isinstance(r.get("setup"), dict) else set()
        if extra: print(f"record {i + 1}: note: new setup keys {sorted(extra)}; document them in SKILL.md", file=sys.stderr)
        if isinstance(r, dict) and r.get("test") in PIPELINE_TESTS and r.get("outcome") not in ("blocked", "expected-stop") and "compute" not in r and "--check" not in sys.argv:
            print(f"record {i + 1}: note: no compute (what serverless was set to, what it ran on: bin/compute_report.sh)", file=sys.stderr)
    for i, p in bad: print(f"record {i + 1}: {p}", file=sys.stderr)
    if bad: sys.exit(1)
    if "--check" in sys.argv: print(f"{len(recs)} records OK"); return
    RUNS.mkdir(parents=True, exist_ok=True)
    for run in dict.fromkeys(r["run"] for r in recs):
        path = RUNS / f"{run}.jsonl"
        with path.open("a") as f:
            for r in recs:
                if r["run"] == run: f.write(json.dumps(r, separators=(",", ":")) + "\n")
        print(f"appended {sum(r['run'] == run for r in recs)} records to {path.relative_to(REPO)}")

if __name__ == "__main__":
    main()
