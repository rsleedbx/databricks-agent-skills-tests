"""lfc-slot [--owner <owner>] <command> [args]: test databases for several people on one VM, one container each, with
the data on JuiceFS (object storage) so a stopped database costs no disk or CPU. Run as root on the VM.
  masters                                   the masters a database can start from ($ROOT/masters)
  ls [--all]                                your databases (--all: everyone's): engine, state, port, created, from
  up <engine> <name> [--from <master> | --empty] [--park | --over-limit] [--edition <edition>]
                                            start <name>: resume it if it exists, else clone a master (juicefs clone
                                            or an XFS reflink) or start empty. Engines: postgres, mysql, sqlserver,
                                            oracle, mariadb, db2, informix, cockroachdb, yugabytedb.
                                            At your limit of running databases (limits), a soft limit: exit 3 naming
                                            them, so the client asks you; --over-limit starts another anyway, --park
                                            parks the ones that started first. The VM's limit (MAX_RUNNING) is hard.
                                            --edition (SQL Server, a new database): Developer (default), Express,
                                            Standard, Enterprise, Evaluation or EnterpriseCore, the container's
                                            MSSQL_PID, kept in the record. Express has no SQL Server Agent: CDC's
                                            capture jobs don't run there; change tracking works.
                                            Prints one JSON line: contract, name, engine, host, port, user, password,
                                            container, state, and service (Oracle: lfc_user's PDB) and database.
                                            A database cloned from a master gets a new admin password at its first
                                            start (cockroachdb, yugabytedb: insecure, none)
  park <name>                               stop it; the data stays on JuiceFS
  clone <name> <new name>                   copy <name> to a new parked database (juicefs clone or cp --reflink: it
                                            shares blocks, seconds); a running <name> stops for the copy, then starts
  rm <name>                                 stop it and delete its data
  usage [--days <n>] [--all]                hours running per owner in the last n days (default 7), from the ledger
  limits                                    your limits, one JSON line: max_running (0 = none), hours_per_week
  sql <name> [database] [--wide]            SQL from stdin as the engine's admin in your running database: rows
                                            tab-separated, no headers, the SQL's exit status. --wide: SQL Server
                                            prints long values whole. Oracle and Db2 take no database
  info <name>                               the host OS, Podman, the image and digest, and the VM
  facts <name>                              one JSON line for a pipeline diagram: the VM, the container, JuiceFS
--owner: who the database belongs to. A forced SSH command sets it per key (slot-ssh.sh), so a user can't pick another
owner; as root without --owner the owner is "admin".
Settings, from /etc/lfc-slot.env (over the environment): HOST, ROOT (/mnt/jfs), STATE (/var/lib/lfc/slots),
PORT_MIN/PORT_MAX (30000/30063), CPUS_PER_DB (2), MAX_RUNNING (vCPUs / CPUS_PER_DB, at least 1), MAX_PER_OWNER (1;
0 = none), SECRETS (/root/lfc-src), LOCK_WAIT (600 s), LOCK ($STATE/.lock: on a local disk), LIMITS
(/etc/lfc-slot.limits: per owner, read on every command: "default max_running=1 hours_per_week=29", "alice max_running=4").
"""
from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import math
import os
import re
import secrets
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from . import engines
from .engines import Engine
from .host import Fail, Host, Lock

NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
ENV_FILE = os.environ.get("LFC_SLOT_ENV", "/etc/lfc-slot.env")


def read_env_file(path: str) -> dict[str, str]:
    """KEY=VALUE lines of a shell env file (export, quotes and comments allowed)."""
    out: dict[str, str] = {}
    try:
        text = Path(path).read_text()
    except OSError:
        return out
    for line in text.splitlines():
        m = re.match(r"^\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)$", line)
        if m:
            v = m.group(2).strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                v = v[1:-1]
            else:
                v = v.split(" #", 1)[0].strip()
            out[m.group(1)] = v
    return out


@dataclass
class Settings:
    root: str
    state: str
    secrets: str
    port_min: int
    port_max: int
    max_per_owner: int
    cpus_per_db: int
    max_running: int
    lock: str
    lock_wait: float
    limits: str
    host: str

    @classmethod
    def load(cls, env=None) -> Settings:
        env = dict(os.environ if env is None else env)
        env.update(read_env_file(ENV_FILE))         # the file wins, as slot.sh sourced it over the environment
        g = lambda k, d: env.get(k) or d
        state = g("STATE", "/var/lib/lfc/slots")
        cpus = int(g("CPUS_PER_DB", "2"))
        return cls(root=g("ROOT", "/mnt/jfs"), state=state, secrets=g("SECRETS", "/root/lfc-src"),
                   port_min=int(g("PORT_MIN", "30000")), port_max=int(g("PORT_MAX", "30063")),
                   max_per_owner=int(g("MAX_PER_OWNER", "1")), cpus_per_db=cpus,
                   max_running=max(1, int(g("MAX_RUNNING", str((os.cpu_count() or 1) // cpus)))),
                   lock=g("LOCK", f"{state}/.lock"), lock_wait=float(g("LOCK_WAIT", "600")),
                   limits=g("LIMITS", "/etc/lfc-slot.limits"), host=env.get("HOST", ""))


def limit(path: str, owner: str, key: str, fallback: str) -> str:
    """key=value on owner's line of the limits file, else on its "default" line, else fallback."""
    try:
        lines = [ln.split() for ln in Path(path).read_text().splitlines()]
    except OSError:
        lines = []
    for who in (owner, "default"):
        v = ""
        for f in lines:
            if f and f[0] == who:
                for kv in f[1:]:
                    if kv.startswith(key + "="):
                        v = kv[len(key) + 1:]
        if v:
            return v
    return fallback


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ts(s: str) -> float:
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


def tenth(x: float):
    """jq's (x * 10 | round / 10): half away from zero, and a whole number without .0."""
    r = math.floor(abs(x) * 10 + 0.5) / 10 * (1 if x >= 0 else -1)
    return int(r) if r == int(r) else r


def usage_report(events: list[dict], since: str, now_s: str, me: str, everyone: bool) -> list[dict]:
    """Hours running per owner and database since `since`, from the ledger's up/park/rm events (an open up counts to
    now)."""
    t0, tn = ts(since), ts(now_s)
    groups: dict[tuple[str, str], list[dict]] = {}
    for e in events:
        groups.setdefault((e["owner"], e["name"]), []).append(e)
    per: dict[str, list[dict]] = {}
    for (o, n) in sorted(groups, key=lambda k: k[0] + "/" + k[1]):
        start, secs = None, 0.0
        for e in groups[(o, n)]:
            if e["event"] == "up":
                start = start or e["ts"]
            elif start is not None:
                secs += max(0.0, max(ts(e["ts"]), t0) - max(ts(start), t0))
                start = None
        if start is not None:
            secs += tn - max(ts(start), t0)
        if everyone or o == me:
            per.setdefault(o, []).append({"name": n, "secs": secs})
    return [{"owner": o, "since": since[:10], "hours": tenth(sum(d["secs"] for d in ds) / 3600),
             "databases": [{"name": d["name"], "hours": tenth(d["secs"] / 3600)} for d in ds]}
            for o, ds in sorted(per.items())]


def say(msg: str) -> None:
    print(f"slot: {msg}", file=sys.stderr)


def new_password() -> str:
    return "Lfc1" + secrets.token_hex(12)


class Slots:
    def __init__(self, owner: str, settings: Settings, host: Host | None = None):
        self.owner, self.s, self.h = owner, settings, host or Host()
        self.ledger = f"{settings.state}/ledger.jsonl"

    # ---- names and records
    def cname(self, name: str, owner: str | None = None) -> str:
        return f"lfc-slot-{owner or self.owner}-{name}"

    def file(self, name: str, ext: str, owner: str | None = None) -> Path:
        return Path(self.s.state) / (owner or self.owner) / f"{name}{ext}"

    def data(self, name: str) -> str:
        return f"{self.s.root}/dbs/{self.owner}/{name}"

    def record(self, name: str) -> dict:
        p = self.file(name, ".json")
        if not p.is_file():
            raise Fail(f"no database {name}")
        return json.loads(p.read_text())

    def records(self) -> list[tuple[str, str, dict]]:
        """(owner, name, record) of every database on the VM."""
        out = []
        for p in sorted(Path(self.s.state).glob("*/*.json")):
            try:
                out.append((p.parent.name, p.stem, json.loads(p.read_text())))
            except ValueError:
                continue
        return out

    def log(self, name: str, event: str, owner: str | None = None) -> None:
        with open(self.ledger, "a") as f:
            f.write(json.dumps({"ts": now(), "owner": owner or self.owner, "name": name, "event": event},
                               separators=(",", ":")) + "\n")

    def lock(self) -> Lock:
        return Lock(self.s.lock, self.s.lock_wait)

    @staticmethod
    def check_name(n: str) -> str:
        if not NAME.match(n or ""):
            raise Fail(f"bad name: {n} (lowercase letters, digits, -)")
        return n

    def prepare(self) -> None:
        """The state folders (and STATE on ROOT needs ROOT mounted)."""
        if self.s.state.startswith(self.s.root + "/") and not self.h.ok(["mountpoint", "-q", self.s.root]):
            raise Fail(f"{self.s.root} isn't mounted: the state is on it")
        for d in (self.s.state, f"{self.s.state}/{self.owner}", os.path.dirname(self.s.lock)):
            os.makedirs(d, exist_ok=True)
            os.chmod(d, 0o700)

    # ---- the files a database's start reads
    def fs_clone(self, src: str, dst: str) -> None:
        """A copy sharing blocks with src: juicefs clone on a JuiceFS ROOT (metadata only), else cp --reflink=always
        (XFS with reflink); never a silent full copy."""
        if self.h.fstype(self.s.root) == "fuse.juicefs":
            if self.h.run(["juicefs", "clone", "--preserve", src, dst], quiet=False).returncode:
                raise Fail(f"juicefs clone {src} {dst} failed")
        elif self.h.run(["cp", "-a", "--reflink=always", src, dst]).returncode:
            shutil.rmtree(dst, ignore_errors=True)
            raise Fail(f"no reflink copy on {self.s.root}: it must be XFS with reflink=1 (disk-setup.sh)")

    def free_port(self) -> int:
        used = {int(r.get("port") or 0) for _, _, r in self.records()} | set(self.h.listening_ports())
        for p in range(self.s.port_min, self.s.port_max + 1):
            if p not in used:
                return p
        raise Fail(f"no free port in {self.s.port_min}-{self.s.port_max}")

    def park_one(self, name: str, owner: str | None = None) -> None:
        self.h.stop(self.cname(name, owner))
        self.log(name, "park", owner)

    # ---- the engine inside its container
    def sql(self, c: str, e: Engine, text: str, timeout: float | None = None, quiet: bool = True):
        words, opts = engines.admin_cmd(e, self.h.ip(c) if e.name == "yugabytedb" else "")
        return self.h.exec(c, words, engines.admin_input(e, text), opts, timeout=timeout, quiet=quiet)

    def db2_db(self, c: str) -> bool:
        return self.h.exec(c, ["su", "-", "db2inst1", "-c", engines.DB2_DB]).returncode == 0

    def ready(self, c: str, e: Engine) -> bool:
        line = engines.READY_LOG.get(e.name)
        if line and line not in self.h.logs(c):
            return False
        if e.name == "db2" and not self.db2_db(c):
            return False
        return self.sql(c, e, engines.probe(e), timeout=30).returncode == 0

    def start_db(self, name: str, e: Engine, rec: dict) -> None:
        """The container from the record and the env file: every start, so the env file's current values apply."""
        c, d = self.cname(name), self.data(name)
        self.h.remove(c)
        runopts = list(e.runopts)
        if e.name == "sqlserver" and rec.get("edition"):
            runopts = [o.replace("MSSQL_PID=Developer", f"MSSQL_PID={rec['edition']}") for o in runopts]
        vols = ["-v", f"{d}:{e.mnt}:{e.volopt}"] + (["-v", f"{d}/cfg:/opt/ibm/config:U,Z"] if e.name == "informix" else [])
        argv = ["podman", "run", "-d", "-q", "--name", c, "--label", f"lfc-slot.owner={self.owner}",
                "--label", f"lfc-slot.name={name}", "--label", f"lfc.engine={e.name}", "--label", "lfc.purpose=slot",
                *runopts, "--env-file", str(self.file(name, ".env")), "-p", f"{rec['port']}:{e.cport}", *vols,
                e.image, *e.args]
        if self.h.run(argv).returncode:
            raise Fail(f"podman run {c} failed")

    def await_db(self, c: str, e: Engine) -> None:
        """Until the engine answers (its wait, in wall time: each probe gives up after 30 s)."""
        end = time.monotonic() + e.wait
        while time.monotonic() < end:
            if self.ready(c, e):
                return
            if not self.h.running(c):
                raise Fail(f"{e.name} in {c} stopped: podman logs {c}")
            time.sleep(2)
        if not self.ready(c, e):
            raise Fail(f"{e.name} in {c} didn't answer in {e.wait // 60} minutes: podman logs {c}")

    def informix_os(self, c: str, env: Path) -> bool:
        """The Informix admin's OS password (user informix) from its env file."""
        pw = read_env_file(str(env)).get("INFORMIX_PASSWORD", "")
        return self.h.exec(c, ["chpasswd"], f"informix:{pw}\n", ["-u", "root"]).returncode == 0

    def prep(self, e: Engine, c: str, login: str, env: Path) -> bool:
        """What a start needs before lfc_user's login (idempotent, every start): the OS users of Db2 and Informix, the
        Informix admin's OS password and its test database."""
        if e.name not in ("db2", "informix"):
            return True
        if e.name == "informix" and not self.informix_os(c, env):
            return False
        if self.h.exec(c, ["sh", "-c", engines.OS_USER], f"lfc_user:{login}\n", ["-u", "root"]).returncode:
            return False
        if e.name == "informix":
            r = self.sql(c, e, "SELECT name FROM sysmaster:sysdatabases WHERE name = 'lfcdb';\n")
            if "lfcdb" not in (r.stdout or ""):
                self.sql(c, e, "CREATE DATABASE lfcdb WITH LOG;\n")
        return True

    # ---- the admin password of a clone
    def rotate_change(self, e: Engine, c: str, old: str, new: str) -> bool:
        """The admin's password changed inside the database. The old one logs in first, from stdin (never on a command
        line)."""
        x = lambda words, inp, opts=(): self.h.exec(c, words, inp, opts)
        n = e.name
        if n == "postgres":
            return x(["sh", "-c", "read -r p; PGPASSWORD=$p exec psql -h 127.0.0.1 -U postgres -d postgres "
                                  "-v ON_ERROR_STOP=1 -q"],
                     f"{old}\nALTER USER postgres WITH PASSWORD '{new}';\n").returncode == 0
        if n in ("mysql", "mariadb"):
            # every root account gets it (the image makes root@% and root@localhost, or one), in one statement, so a
            # failure leaves no account with the new one
            cli = f"read -r p; MYSQL_PWD=$p exec {n if n == 'mariadb' else 'mysql'} -h127.0.0.1 -uroot"
            r = x(["sh", "-c", cli + " -N -B"], f"{old}\nSELECT host FROM mysql.user WHERE user = 'root';\n")
            hosts = (r.stdout or "").split()
            if r.returncode or not hosts:
                return False
            alter = "ALTER USER " + ", ".join(f"'root'@'{h}' IDENTIFIED BY '{new}'" for h in hosts) + ";"
            return x(["sh", "-c", cli], f"{old}\n{alter}\n").returncode == 0
        if n == "sqlserver":
            return x(["sh", "-c", f"read -r p; f=$(mktemp); cat > $f; SQLCMDPASSWORD=$p {engines.SQLCMD} -C -S localhost "
                                  "-U sa -b -i $f; r=$?; rm -f $f; exit $r"],
                     f"{old}\nALTER LOGIN sa WITH PASSWORD = N'{new}';\n").returncode == 0
        if n == "oracle":
            r = self.h.run(["podman", "exec", "-i", c, "sqlplus", "-s", "-L", "/nolog"], quiet=True,
                           input=f'whenever sqlerror exit failure\nCONNECT sys/"{old}"@//localhost:1521/FREE AS SYSDBA\n'
                                 f'ALTER USER sys IDENTIFIED BY "{new}";\nALTER USER system IDENTIFIED BY "{new}";\nexit\n')
            # whenever sqlerror misses a failed CONNECT (an SQL*Plus command) and the "not connected" errors after it
            return r.returncode == 0 and "ORA-" not in r.stdout and "SP2-" not in r.stdout
        if n == "db2":
            ok = x(["bash", "-c", 'read -r p; f=$(mktemp); printf "connect to LFCDB user db2inst1 using %s;\\n" "$p" > $f; '
                                  'chown db2inst1 $f; su - db2inst1 -c "db2 -x -t -s -f $f" >/dev/null 2>&1; r=$?; '
                                  'rm -f $f; exit $r'], f"{old}\n").returncode == 0
            return ok and x(["sh", "-c", 'read -r l; echo "$l" | chpasswd'], f"db2inst1:{new}\n",
                            ["-u", "root"]).returncode == 0
        if n == "informix":     # prep() re-applies the env file's INFORMIX_PASSWORD at every start
            return (x(["unix_chkpwd", "informix", "nullok"], f"{old}\0", ["-u", "root"]).returncode == 0
                    and x(["chpasswd"], f"informix:{new}\n", ["-u", "root"]).returncode == 0)
        return False

    def rotate_admin(self, e: Engine, c: str, env: Path) -> bool:
        """The clone's admin password becomes a new one. The new value goes to a file beside the env file first; the env
        file is replaced only after the change succeeded. A failure keeps the tried value as <env>.pending and never
        replaces it (Oracle changes sys, then system: a failure between leaves sys with it); a retry tries that same
        value, logging in with the old password or, when the database has the pending one already, with that."""
        k = e.admin_key
        if not k:
            return True
        pend = Path(f"{env}.pending")
        old = read_env_file(str(env)).get(k, "")
        if not old:
            return False
        new = read_env_file(str(pend)).get(k, "") if pend.exists() else ""
        new = new or new_password()
        fd, t = tempfile.mkstemp(prefix=env.name + ".", dir=str(env.parent))
        with os.fdopen(fd, "w") as f:
            f.write("".join(f"{k}={new}\n" if ln.startswith(k + "=") else ln + "\n"
                            for ln in env.read_text().splitlines()))
        os.chmod(t, 0o600)
        if not (self.rotate_change(e, c, old, new) or (pend.exists() and self.rotate_change(e, c, new, new))):
            if pend.exists():
                os.unlink(t)
            else:
                os.replace(t, pend)
            say(f"if part of the change went through, the tried admin password is in {pend}")
            return False
        try:
            os.replace(t, env)
        except OSError:
            say(f"the new admin password is in {t}: move it over {env}")
            return False
        with contextlib.suppress(FileNotFoundError):
            pend.unlink()
        return True

    def rotate_now(self, name: str, e: Engine, rec: dict) -> None:
        """A clone's first start: its admin password changes once, here; the engines whose admin helpers read the
        container's env (and Db2's image, at start) are started again from the new env file."""
        mark, c = self.file(name, ".rotate"), self.cname(name)
        if not e.admin_key:
            mark.unlink()
            say(f"{e.name} is insecure: no admin password to change")
        elif self.rotate_admin(e, c, self.file(name, ".env")):
            mark.unlink()
            say(f"{name}: its admin password is a new one (the master's is no longer valid for it)")
            if e.restart_after_rotate:
                self.start_db(name, e, rec)
                self.await_db(c, e)
        else:
            say(f"{name}: its admin password is still the master's (the change failed): up again to retry")

    # ---- commands
    def masters(self) -> list[str]:
        return sorted(p.name for p in Path(self.s.root, "masters").glob("*") if p.is_dir())

    def ls(self, everyone: bool = False) -> str:
        fmt = "{:<12} {:<20} {:<10} {:<8} {:<6} {:<20} {}"
        rows = [fmt.format("owner", "name", "engine", "state", "port", "created", "from")]
        for o, n, r in self.records():
            if everyone or o == self.owner:
                st = "running" if self.h.running(self.cname(n, o)) else "parked"
                rows.append(fmt.format(o, n, r.get("engine", ""), st, r.get("port", ""), r.get("created", ""),
                                       r.get("from", "")))
        return "\n".join(rows)

    def per_owner(self) -> int:
        v = limit(self.s.limits, self.owner, "max_running", str(self.s.max_per_owner))
        if not v.isdigit():
            raise Fail(f"max_running for {self.owner} in {self.s.limits} isn't a number: {v}")
        return int(v)

    def room(self, name: str, park: bool, over: bool) -> None:
        """The owner's limit of running databases (soft: exit 3), then the VM's (hard)."""
        per = self.per_owner()
        if per > 0:
            mine = sorted((self.h.inspect(self.cname(n), "{{.State.StartedAt}}"), n) for o, n, _ in self.records()
                          if o == self.owner and n != name and self.h.running(self.cname(n)))
            mine = [n for _, n in mine]
            if len(mine) >= per:
                names = " ".join(mine)
                if park:
                    for n in mine[:len(mine) - per + 1]:
                        say(f"parking your running database {n} first ({per} at a time for you on this VM)")
                        self.park_one(n)
                elif over:
                    say(f"you have {len(mine)} of {per} running databases on this VM ({names}): starting {name} as "
                        "well (--over-limit)")
                else:   # a soft limit: the client asks the person (lfc db up asks; an agent asks before --over-limit)
                    say(f"you have {len(mine)} of {per} running databases on this VM ({names}): start {name} as well "
                        "(up again with --over-limit), park one first (park <name>), or up again with --park to park "
                        "the one that started first")
                    raise Fail(None, 3)
        run_now = self.h.running_names()
        if len(run_now) >= self.s.max_running:
            nxt = (f"park one, or a bigger VM (one database per {self.s.cpus_per_db} vCPU)" if self.s.max_per_owner == 0
                   else "try later, or ask for a VM of your own")
            names = " ".join(n[len("lfc-slot-"):] for n in run_now)
            raise Fail(f"the VM is full: {len(run_now)} of {self.s.max_running} databases are running ({names}); {nxt}")

    def create(self, name: str, e: Engine, src: str, edition: str) -> dict:
        """A new database: its data (a master's clone, or empty), admin env, login password, port and record."""
        d = self.data(name)
        if os.path.isdir(d):
            raise Fail(f"{d} exists without a record; remove it or pick another name")
        os.makedirs(f"{self.s.root}/dbs/{self.owner}", exist_ok=True)
        os.chmod(f"{self.s.root}/dbs/{self.owner}", 0o755)
        env = self.file(name, ".env")
        old = os.umask(0o077)
        try:
            if src:
                if not re.match(r"^[A-Za-z0-9._-]+$", src) or not os.path.isdir(f"{self.s.root}/masters/{src}"):
                    raise Fail(f"no master {src} (lfc-slot masters)")
                admin = Path(self.s.secrets, f"{e.name}.env")
                if not admin.is_file():
                    raise Fail(f"no admin env {admin} for the {e.name} masters")
                self.fs_clone(f"{self.s.root}/masters/{src}", d)
                shutil.copyfile(admin, env)
                self.file(name, ".rotate").write_text("")   # the master's admin password: the first start changes it
            else:
                os.makedirs(d)
                if e.data_owner and self.h.run(["chown", e.data_owner, d]).returncode:
                    raise Fail(f"chown {e.data_owner} {d} failed")
                if e.name == "informix":
                    os.makedirs(f"{d}/cfg")                 # the server's config volume
                env.write_text(engines.new_env(e, new_password()))
            self.file(name, ".login").write_text(new_password() + "\n")
            rec = {"engine": e.name, "port": self.free_port(), "created": now(), "from": src or "empty",
                   "image": e.image, **({"edition": edition} if edition else {})}
            self.file(name, ".json").write_text(json.dumps(rec, indent=2) + "\n")
        finally:
            os.umask(old)
        return rec

    def up(self, eng: str, name: str, src: str = "", empty: bool = False, park: bool = False, over: bool = False,
           edition: str = "") -> dict:
        self.check_name(name)
        try:
            e = engines.get(eng)
        except KeyError as x:
            raise Fail(x.args[0])
        if edition:
            if e.name != "sqlserver":
                raise Fail("--edition is for sqlserver only")
            if edition not in engines.EDITIONS:
                raise Fail(f"edition {edition}: {', '.join(engines.EDITIONS[:-1])} or {engines.EDITIONS[-1]}")
            if edition == "Express":
                say("Express has no SQL Server Agent: CDC won't capture changes there; change tracking works")
        c, meta = self.cname(name), self.file(name, ".json")
        with self.lock():
            rec = json.loads(meta.read_text()) if meta.is_file() else None
            if rec:
                if rec.get("engine") != e.name:
                    raise Fail(f"{name} is a {rec.get('engine')} database")
                have = rec.get("edition") or "Developer"
                if edition and edition != have:
                    raise Fail(f"{name} is a SQL Server {have} database (the edition is set when it is made)")
            if not self.h.running(c):
                self.room(name, park, over)
                if not rec:
                    if not src and not empty:
                        raise Fail("new database: --from <master> or --empty (lfc-slot masters)")
                    rec = self.create(name, e, src, edition)
                self.start_db(name, e, rec)
            # every up, started or already running: an up cut short (its ssh dropped during a first start) leaves a
            # running container without lfc_user, so the login runs again here (idempotent)
            self.await_db(c, e)
            env, login = self.file(name, ".env"), self.file(name, ".login").read_text().strip("\n")
            if self.file(name, ".rotate").exists():
                if e.name == "informix" and not self.informix_os(c, env):   # the check reads the env file's password
                    self.h.remove(c)
                    raise Fail(f"the Informix admin's OS password in {c} failed; run up again")
                self.rotate_now(name, e, rec)
            if not self.prep(e, c, login, env):
                self.h.remove(c)
                raise Fail(f"preparing {e.name} in {c} failed; run up again")
            # a failed login stops the container, so the next up tries again instead of resuming without lfc_user
            if self.sql(c, e, engines.login_sql(e, login)).returncode:
                self.h.remove(c)
                raise Fail(f"creating lfc_user in {c} failed; run up again")
            self.log(name, "up")
        out = {"contract": 1, "name": name, "engine": e.name, "host": self.s.host or self.h.public_ip(),
               "port": int(rec["port"]), "user": "lfc_user", "password": login, "container": c, "state": "running"}
        out.update({"service": e.service} if e.service else {})
        out.update({"database": e.dbname} if e.dbname else {})
        return out

    def park(self, name: str) -> str:
        self.check_name(name)
        self.record(name)
        with self.lock():
            if not self.h.running(self.cname(name)):
                return f"{name} is already parked"
            self.park_one(name)
        return f"parked {name}: its data stays in {self.data(name)}"

    def clone(self, name: str, new: str) -> dict:
        self.check_name(name)
        self.check_name(new)
        if name == new:
            raise Fail("clone to a new name")
        rec = self.record(name)
        lk = self.lock()
        with lk:
            if self.file(new, ".json").exists():
                raise Fail(f"{new} exists")
            if os.path.exists(self.data(new)):
                raise Fail(f"{self.data(new)} exists without a record; remove it or pick another name")
            was = self.h.running(self.cname(name))
            if was:
                say(f"stopping {name} for a consistent copy")
                self.park_one(name)
            t0 = time.time()
            old = os.umask(0o077)
            try:
                self.fs_clone(self.data(name), self.data(new))
                shutil.copyfile(self.file(name, ".env"), self.file(new, ".env"))   # the admin password is in the data
                self.file(new, ".rotate").write_text("")                          # ... and the copy gets a new one
                self.file(new, ".login").write_text(new_password() + "\n")
                nrec = dict(rec, port=self.free_port(), created=now(), **{"from": f"clone:{name}"})
                self.file(new, ".json").write_text(json.dumps(nrec, indent=2) + "\n")
            finally:
                os.umask(old)
            self.log(new, "clone")
            secs = int(time.time() - t0)
        if was:
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    self.up(rec["engine"], name)
            except Fail as x:
                if x.msg:
                    say(x.msg)
                say(f"{name} didn't start again: up {rec['engine']} {name}")
        say(f"up {rec['engine']} {new} starts it")
        return {"name": new, "engine": rec["engine"], "from": name, "port": int(nrec["port"]), "state": "parked",
                "seconds": secs}

    def rm(self, name: str) -> str:
        self.check_name(name)
        self.record(name)
        with self.lock():
            if self.h.running(self.cname(name)):
                self.park_one(name)
            d = self.data(name)
            if os.path.isdir(d) and not self.h.ok(["juicefs", "rmr", d]):
                shutil.rmtree(d, ignore_errors=True)
            for ext in (".json", ".env", ".env.pending", ".login", ".rotate"):
                with contextlib.suppress(FileNotFoundError):
                    self.file(name, ext).unlink()
            self.log(name, "rm")
        return f"removed {name}"

    def limits(self) -> dict:
        per = self.per_owner()
        h = limit(self.s.limits, self.owner, "hours_per_week", "")
        return {"owner": self.owner, "max_running": per,
                "hours_per_week": None if h == "" else (int(h) if h.isdigit() else float(h))}

    def usage(self, days: int, everyone: bool) -> list:
        if not os.path.isfile(self.ledger):
            return []
        events = [json.loads(ln) for ln in Path(self.ledger).read_text().splitlines() if ln.strip()]
        since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return usage_report(events, since, now(), self.owner, everyone)

    def run_sql(self, name: str, database: str, wide: bool) -> int:
        """stdin as the admin in the running database (no lock: a long fill mustn't block up or park)."""
        self.check_name(name)
        rec = self.record(name)
        if database and not re.match(r"^[A-Za-z0-9_]{1,128}$", database):
            raise Fail(f"bad database: {database} (letters, digits, _)")
        e, c = engines.get(rec["engine"]), self.cname(name)
        if not self.h.running(c):
            raise Fail(f"{name} is parked: up {e.name} {name} first")
        if not e.takes_database and database:
            raise Fail(f"{e.name} takes no database argument")
        ip = self.h.ip(c) if e.name == "yugabytedb" else ""
        words, opts = (engines.admin_cmd(e, ip) if not e.takes_database else (engines.table_cmd(e, database, wide, ip), []))
        head, tail = (b"whenever sqlerror exit failure\n", b"\nexit\n") if e.name == "oracle" else (b"", b"")
        return self.h.exec_stream(c, words, opts, head, tail)

    def info(self, name: str) -> str:
        self.check_name(name)
        rec, c = self.record(name), self.cname(name)
        osr = read_env_file("/etc/os-release").get("PRETTY_NAME", "unknown")
        lines = [f"Host: {osr}", f"Podman: {self.h.out(['podman', '--version']).strip()}"]
        lines.append(self.h.inspect(c, "Image: {{.ImageName}} ({{.ImageDigest}})") if self.h.running(c)
                     else f"Image: {rec.get('image', '')} (parked)")
        lines.append(f"Data: JuiceFS volume at {self.s.root} (object storage), database {rec.get('engine')} {name}, "
                     f"port {rec.get('port')}")
        az = self.h.azure_compute()
        if az:
            lines.append(f"VM: Azure {az.get('vmSize')}, region {az.get('location')}, zone {az.get('zone') or 'none'}")
        return "\n".join(lines)

    def facts(self, name: str) -> dict:
        self.check_name(name)
        rec, c, d = self.record(name), self.cname(name), self.data(name)
        cpu = os.cpu_count() or 0
        ram = 0
        with contextlib.suppress(OSError):
            for ln in Path("/proc/meminfo").read_text().splitlines():
                if ln.startswith("MemTotal:"):
                    ram = int(int(ln.split()[1]) / 1048576 + 0.5)
        az = self.h.azure_compute()
        if az:
            sp = az.get("storageProfile") or {}
            num = lambda v: int(v) if str(v).isdigit() else None
            disks = [{"role": "os", "gb": num((sp.get("osDisk") or {}).get("diskSizeGB")),
                      "type": ((sp.get("osDisk") or {}).get("managedDisk") or {}).get("storageAccountType")}]
            disks += [{"role": "data", "gb": num(x.get("diskSizeGB")),
                       "type": (x.get("managedDisk") or {}).get("storageAccountType")} for x in sp.get("dataDisks") or []]
            vm = {"cloud": "azure", "type": az.get("vmSize"), "region": az.get("location"), "cpu": cpu, "ram_gb": ram,
                  "disks": disks}
            backend = "Azure Blob"
        else:
            it = self.h.aws("instance-type") or "unknown"
            disks = []
            for ln in self.h.out(["lsblk", "-b", "-d", "-n", "-o", "NAME,SIZE,ROTA,TYPE"]).splitlines():
                f = ln.split()
                if len(f) == 4 and f[3] == "disk":
                    disks.append({"role": "disk", "name": f[0], "gb": int(int(f[1]) / 1e9 + 0.5),
                                  "type": "HDD" if f[2] == "1" else "SSD"})
            vm = {"cloud": "" if it == "unknown" else "aws", "type": it, "region": self.h.aws("placement/region"),
                  "cpu": cpu, "ram_gb": ram, "disks": disks}
            backend = "Amazon S3"
        jfs = []
        for ln in self.h.out(["findmnt", "-n", "-t", "fuse.juicefs", "-o", "SOURCE,TARGET"]).splitlines():
            f = ln.split()
            if len(f) >= 2:
                jfs.append({"name": re.sub(r"^JuiceFS:", "", f[0]), "mount": f[1], "backend": backend})
        db = {"container": c, "engine": rec.get("engine"), "state": "parked", "cpu": None, "ram_gb": None,
              "engine_ram_gb": None, "data_mb": None, "data_on": "disk"}
        if self.h.running(c):
            db["state"] = "running"
            lim = self.h.inspect(c, "{{.HostConfig.NanoCpus}} {{.HostConfig.CpuQuota}} {{.HostConfig.CpuPeriod}} "
                                    "{{.HostConfig.Memory}}").split() + ["0"] * 4
            nano, quota, period, mem = (int(x) if x.lstrip("-").isdigit() else 0 for x in lim[:4])
            db["cpu"] = nano / 1e9 if nano else (quota / period if quota > 0 and period else None)
            db["ram_gb"] = tenth(mem / 1073741824) if mem else None
            env = self.h.inspect(c, "{{range .Config.Env}}{{println .}}{{end}}")
            m = re.search(r"^MSSQL_MEMORY_LIMIT_MB=(\d+)", env, re.MULTILINE)
            db["engine_ram_gb"] = tenth(int(m.group(1)) / 1024) if m else None
        du = self.h.out(["du", "-sm", d]).split()
        db["data_mb"] = int(du[0]) if du and du[0].isdigit() else None
        if self.h.out(["findmnt", "-n", "-t", "fuse.juicefs", "-T", d]).strip():
            db["data_on"] = "juicefs"
        return {"source_host": "VM + Podman", "vm": vm, "db": db, "juicefs": jfs}


def jline(x) -> str:
    return json.dumps(x, separators=(",", ":"))


def usage_exit() -> None:
    print(__doc__.strip(), file=sys.stderr)
    raise Fail(None, 2)


def main(argv: list[str], host: Host | None = None, env=None) -> int:
    """lfc-slot's commands; exit 0, 1 (a failure, said on stderr), 2 (usage) or 3 (at your limit: ask)."""
    try:
        owner = "admin"
        if argv[:1] == ["--owner"]:
            owner, argv = (argv[1] if len(argv) > 1 else ""), argv[2:]
        if not NAME.match(owner):
            raise Fail(f"bad owner: {owner}")
        if not argv:
            usage_exit()
        cmd, a = argv[0], argv[1:]
        sl = Slots(owner, Settings.load(env), host)
        sl.prepare()
        if cmd == "masters":
            for m in sl.masters():
                print(m)
        elif cmd == "ls":
            print(sl.ls(a[:1] == ["--all"]))
        elif cmd == "up":
            if len(a) < 2:
                usage_exit()
            eng, name, rest, kw = a[0], a[1], a[2:], {}
            while rest:
                o = rest.pop(0)
                if o in ("--from", "--edition") and rest:
                    kw["src" if o == "--from" else "edition"] = rest.pop(0)
                elif o in ("--empty", "--park", "--over-limit"):
                    kw[{"--empty": "empty", "--park": "park", "--over-limit": "over"}[o]] = True
                else:
                    usage_exit()
            print(jline(sl.up(eng, name, **kw)))
        elif cmd == "park" and a:
            print(sl.park(a[0]))
        elif cmd == "clone" and len(a) >= 2:
            print(jline(sl.clone(a[0], a[1])))
        elif cmd == "rm" and a:
            print(sl.rm(a[0]))
        elif cmd == "limits":
            print(jline(sl.limits()))
        elif cmd == "usage":
            days, everyone = "7", False
            while a:
                o = a.pop(0)
                if o == "--days" and a:
                    days = a.pop(0)
                elif o == "--all":
                    everyone = True
                else:
                    usage_exit()
            if not days.isdigit():
                raise Fail("--days is a number")
            print(json.dumps(sl.usage(int(days), everyone), indent=2))
        elif cmd == "sql" and a:
            db, wide = "", False
            for o in a[1:]:
                if o == "--wide":
                    wide = True
                elif not db:
                    db = o
                else:
                    usage_exit()
            return sl.run_sql(a[0], db, wide)
        elif cmd == "info" and a:
            print(sl.info(a[0]))
        elif cmd == "facts" and a:
            print(jline(sl.facts(a[0])))
        else:
            usage_exit()
        return 0
    except Fail as x:
        if x.msg:
            say(x.msg)
        return x.rc
