#!/usr/bin/env python3
"""settings.py setup|show|path|get KEY|set KEY=VALUE...|unset KEY...|require KEY...|checked|defaults|remove-after|env|
workspaces|azure: the answers a test skill asks for once (cloud accounts, region, workspaces, Podman or a VM, catalog,
resource group, owner), kept per user in one file outside the repo, so later runs read them instead of asking or listing
again. File: $LFC_TESTS_SETTINGS, default ~/.lfc/tests/settings.env (mode 600, folder 700); lines KEY=value, shell-quoted
(eval-able by bash), never committed.
  setup         the first run (or a refresh): one parallel pass over what changes rarely (Azure subscriptions, AWS
                profiles and their accounts, GCP projects from the gcloud config, Databricks workspace profiles and
                whether each login works, local Podman), next to what's saved, so every question is asked at once.
                Then `set` the answers and `checked`.
  show          every key and value (no secrets belong here: passwords stay in their own 0600 files)
  get KEY       the value; exit 1 if it isn't set
  set K=V ...   add or replace keys (KEY: capital letters, digits and _)
  require K ... exit 1 and list the missing keys, one per line, so the skill asks for only those. Reads the file only
                (no CLI calls); notes on stderr when the settings were last checked over 30 days ago
  AZ_RESOURCE_GROUP is an existing group, asked for: setup suggests the groups in AZ_SUBSCRIPTION tagged with the az
  user as Owner or Creator. A subscription allows only so many groups, so the skills never create, re-tag or delete one.
  defaults      the defaults the test skills use unless the user says otherwise (the repo's defaults.yaml), one line each,
                and the stated exceptions: what to tell the user before the first test; setup shows them first
  remove-after  the RemoveAfter tag date for a new test resource: today + REMOVE_AFTER_DAYS (default 7), YYYY-MM-DD
  checked       record today as SETTINGS_CHECKED (after setup, or after a failed call made you re-check a value)
  env           export lines for eval: eval "$(python3 settings.py env)"
  workspaces    the workspace profiles of `databricks auth profiles`, numbered, by cloud: host, and whether the login
                works or an account login covers it (* = saved in WORKSPACES or DATABRICKS_PROFILE). Show it and ask
                which ones the user works in often: those become WORKSPACES, the most used DATABRICKS_PROFILE
  azure         the subscriptions this az login reaches (name, id, tenant), the saved one marked "saved", the CLI's
                default marked "default": what to show the user when AZ_SUBSCRIPTION is missing
"""
from __future__ import annotations

import datetime as dt
import os
import re
import shlex
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "cli-setup", "scripts"))
from laptop import have, json_out, out_of, run

DEFAULTS = os.path.join(HERE, "..", "..", "..", "defaults.yaml")
KEY = re.compile(r"^[A-Z_][A-Z0-9_]*$")
LINE = re.compile(r"^([A-Z_][A-Z0-9_]*)=(.*)$")


def path() -> str:
    return os.environ.get("LFC_TESTS_SETTINGS") or os.path.join(os.path.expanduser("~"), ".lfc", "tests", "settings.env")


def read_lines(f: str) -> list[str]:
    if not os.path.isfile(f):
        return []
    with open(f) as fh:
        return fh.read().splitlines()


def unquote(raw: str) -> str:
    """The value a shell-quoted settings line holds (what the bash that wrote it would have evaluated)."""
    return "".join(shlex.split(raw)) if raw.strip() else ""


def value(key: str) -> str:
    """The saved value of key ('' when unset); the last line for the key wins."""
    v = None
    for line in read_lines(path()):
        m = LINE.match(line)
        if m and m.group(1) == key:
            v = unquote(m.group(2))
    return v or ""


def init() -> None:
    d = os.path.dirname(path())
    os.makedirs(d, exist_ok=True)
    os.chmod(d, 0o700)
    f = path()
    if not os.path.exists(f):
        open(f, "w").close()
    os.chmod(f, 0o600)


def set_values(pairs: list[str]) -> int:
    for kv in pairs:
        k, sep, _ = kv.partition("=")
        if not sep or not KEY.match(k):
            print(f"not KEY=VALUE: {kv}", file=sys.stderr)
            return 2
    init()
    new = dict(kv.split("=", 1) for kv in pairs)
    keep = [line for line in read_lines(path()) if not any(line.startswith(k + "=") for k in new)]
    keep += [f"{k}={shlex.quote(v)}" for k, v in new.items()]
    write_settings(keep)
    return 0


def unset_values(keys: list[str]) -> int:
    if not os.path.isfile(path()):
        return 0
    if not all(KEY.match(k) for k in keys):
        return 2
    keep = [line for line in read_lines(path()) if not any(line.startswith(k + "=") for k in keys)]
    write_settings(keep)
    return 0


def write_settings(lines: list[str]) -> None:
    """Replace the file atomically (a temp file in its folder, mode 600, then rename)."""
    f = path()
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(f), prefix=os.path.basename(f) + ".")
    with os.fdopen(fd, "w") as fh:
        fh.write("".join(line + "\n" for line in lines))
    os.chmod(tmp, 0o600)
    os.replace(tmp, f)


def require(keys: list[str]) -> int:
    miss = 0
    for k in keys:
        if not KEY.match(k):
            return 2
        if not value(k):
            print(k)
            miss = 1
    checked = value("SETTINGS_CHECKED")
    if checked:
        d = dt.datetime.strptime(checked, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
        age = int((dt.datetime.now(dt.timezone.utc) - d).total_seconds() // 86400)
        if age > 30:
            print(f"settings: last checked {checked} ({age} days ago): run setup to refresh them", file=sys.stderr)
    return miss


def defaults_lines() -> list[str]:
    data = json_out(["yq", "-o", "json", ".", DEFAULTS]) or {}
    out = [f"- {d['area']}: {d['value']}" for d in data.get("defaults") or []]
    return out + [f"- (exception) {e}" for e in data.get("exceptions") or []]


def remove_after() -> str:
    d = value("REMOVE_AFTER_DAYS")
    days = int(d) if d.isdigit() else 7
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=days)).strftime("%Y-%m-%d")


def env_lines() -> list[str]:
    return ["export " + line for line in read_lines(path()) if re.match(r"^[A-Z_][A-Z0-9_]*=", line)]


def cloud_of(host: str) -> str:
    if "azuredatabricks" in host:
        return "azure"
    return "gcp" if "gcp.databricks" in host else "aws"


def profiles() -> list[dict]:
    data = json_out(["databricks", "auth", "profiles", "-o", "json"], timeout=90) or {}
    return data.get("profiles") or []


def workspaces() -> list[str]:
    """The workspace profiles, numbered by cloud then name: '<n>[*]\\t<cloud>\\t<name>\\t<login>\\t<host>'."""
    mine = "," + value("WORKSPACES") + "," + value("DATABRICKS_PROFILE") + ","
    allp = profiles()
    accounts = [p for p in allp if (p.get("host") or "").startswith("https://accounts.")]
    ws = sorted((p for p in allp if p not in accounts), key=lambda p: (cloud_of(p.get("host") or ""), p.get("name", "")))
    rows = []
    for i, w in enumerate(ws, 1):
        cloud = cloud_of(w.get("host") or "")
        cov = next((a["name"] for a in accounts if a.get("valid") and w.get("account_id") is not None
                    and a.get("account_id") == w.get("account_id")), "")
        if w.get("valid"):
            login = "login ok"
        elif cov:
            login = f"expired, covered by account login {cov}"
        elif cloud == "azure":
            login = "expired; the Azure CLI may cover it"
        else:
            login = "expired"
        star = "*" if f",{w.get('name')}," in mine else ""
        rows.append("\t".join([f"{i}{star}", cloud, w.get("name", ""), login, w.get("host") or ""]))
    return rows


def azure_subscriptions() -> list[str]:
    saved = value("AZ_SUBSCRIPTION")
    subs = json_out(["az", "account", "list", "--all", "-o", "json"], timeout=60) or []
    rows = []
    for s in sorted(subs, key=lambda s: s.get("name", "")):
        marks = (["saved"] if saved and saved in (s.get("id"), s.get("name")) else []) + (["default"] if s.get("isDefault") else [])
        rows.append("\t".join([s.get("name", ""), s.get("id", ""), s.get("tenantId", ""), " ".join(marks)]))
    return rows


def aws_profiles() -> list[str]:
    if not have("aws"):
        return []
    rows = []
    _, names, _ = run(["aws", "configure", "list-profiles"], timeout=30)
    for p in names.split():   # config only: no network
        a = out_of(["aws", "configure", "get", "sso_account_id", "--profile", p], 30) or "?"
        r = out_of(["aws", "configure", "get", "region", "--profile", p], 30) or "?"
        rows.append(f"{p}\t{a}\t{r}")
    return rows


def gcp_configs() -> list[str]:
    if not have("gcloud"):
        return []
    rc, out, _ = run(["gcloud", "config", "configurations", "list",
                      "--format=value(name,properties.core.project,properties.compute.region,is_active)"], timeout=30)
    rows = []
    for line in (out if rc == 0 else "").splitlines():   # config only: no network
        f = (line.split("\t") + ["", "", "", ""])[:4]
        row = f"{f[0]}\t{f[1] or '?'}\t{f[2] or '?'}"
        rows.append(row + ("\tactive" if f[3] == "True" else ""))
    return rows


def podman_lines() -> list[str]:
    if not have("podman"):
        return ["no podman (lfc-source-podman to install, or a cloud VM: lfc-source-cloud-vm)"]
    if sys.platform != "darwin":
        out = out_of(["podman", "--version"], 30) or ""
        return [f"native podman {out.split()[-1] if out else ''}"]
    if not have("limactl"):
        return ["podman on macOS without Lima: see lfc-source-podman"]
    _, out, _ = run(["limactl", "list", "--format", "{{.Name}} {{.Status}}"], timeout=30)
    return [f"lima vm: {line}" for line in out.splitlines()]


def resource_group_lines() -> list[str]:
    rg, sub = value("AZ_RESOURCE_GROUP"), value("AZ_SUBSCRIPTION")
    if not have("az") or not sub:
        return ["(AZ_SUBSCRIPTION not set: choose the subscription first)"]
    lines = []
    me = out_of(["az", "account", "show", "--query", "user.name", "-o", "tsv"], 30) or ""
    if rg:
        loc = out_of(["az", "group", "show", "--subscription", sub, "-n", rg, "--query", "location", "-o", "tsv"], 60)
        lines.append(f"saved: {rg}, exists, in {loc}" if loc else f"saved: {rg}, NOT found in the subscription")
    if not me:
        lines.append("yours: (the az user couldn't be read: az account show; no groups suggested)")
        return lines
    lines.append(f"yours (Owner or Creator tag = {me}):")
    groups = json_out(["az", "group", "list", "--subscription", sub, "-o", "json"], timeout=60) or []
    for g in groups:
        tags = g.get("tags") or {}
        if (tags.get("Owner") or "").lower() == me.lower() or (tags.get("Creator") or "").lower() == me.lower():
            lines.append(f"  {g.get('name', '')}  {g.get('location', '')}")
    return lines


def setup() -> int:
    with ThreadPoolExecutor(max_workers=7) as pool:
        jobs = {
            "rg": pool.submit(resource_group_lines), "azure": pool.submit(azure_subscriptions),
            "aws": pool.submit(aws_profiles), "gcp": pool.submit(gcp_configs),
            "dbx": pool.submit(workspaces), "podman": pool.submit(podman_lines),
        }
        got = {k: j.result() for k, j in jobs.items()}

    def emit(lines: list[str]) -> None:
        print("\n".join(lines))
        print()
    f = path()
    print("== defaults: used unless you say otherwise (tell the user, and ask what to change)")
    emit(defaults_lines())
    print(f"== saved ({f})")
    if os.path.isfile(f):
        emit(read_lines(f))
    else:
        print()
    print("== Azure resource group for every test resource (AZ_RESOURCE_GROUP: an existing one; skills never create or delete a group)")
    emit(got["rg"])
    print("== Azure subscriptions (name, id, tenant, saved/default)")
    emit(got["azure"])
    print("== AWS profiles (profile, account, region)")
    emit(got["aws"])
    print("== GCP configurations (name, project, region, active)")
    emit(got["gcp"])
    print("== Databricks workspace profiles: which do you work in often? (number, cloud, profile, login, host)")
    emit(got["dbx"])
    print("== local Podman")
    emit(got["podman"])
    return 0


def usage() -> int:
    print(__doc__.split("\n\n")[0], file=sys.stderr)
    print("usage: settings.py setup|show|path|get KEY|set K=V...|unset K...|require K...|checked|defaults|remove-after|"
          "env|workspaces|azure", file=sys.stderr)
    return 2


def main(argv: list[str]) -> int:
    if not argv:
        return usage()
    cmd, args = argv[0], argv[1:]
    if cmd == "path":
        print(path())
    elif cmd == "show":
        for line in read_lines(path()):
            print(line)
    elif cmd == "get":
        if len(args) != 1 or not KEY.match(args[0]):
            return usage()
        v = value(args[0])
        if not v:
            return 1
        print(v)
    elif cmd == "set":
        return set_values(args) if args else usage()
    elif cmd == "unset":
        return unset_values(args) if args else usage()
    elif cmd == "require":
        return require(args) if args else usage()
    elif cmd == "defaults":
        for line in defaults_lines():
            print(line)
    elif cmd == "remove-after":
        print(remove_after())
    elif cmd == "checked":
        return set_values([f"SETTINGS_CHECKED={dt.datetime.now(dt.timezone.utc):%Y-%m-%d}"])
    elif cmd == "setup":
        return setup()
    elif cmd == "workspaces":
        for row in workspaces():
            print(row)
    elif cmd == "env":
        for line in env_lines():
            print(line)
    elif cmd == "azure":
        for row in azure_subscriptions():
            print(row)
    else:
        return usage()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
