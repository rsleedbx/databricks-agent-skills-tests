"""lfc-vm masters <build|export|import>: the master databases a slot clones from (ROOT/masters).

  build <engine> <master> [workload ...]   build a master holding TPC workloads (default all six: tpcb tpcc tpch tpcds
                                           tpce tpcdi), filled inside the database by lfc-source-datagen, stopped cleanly,
                                           and copied into OUT (/mnt/jfs/masters) as <master>, with <master>.engine
  export                                   each master to a Blob container as <master>.tar.zst (tar --numeric-owner,
                                           zstd), each engine's admin env as <engine>.env, and manifest.json
  import                                   the manifest's masters into ROOT/masters and its admin envs into SECRETS

build: each workload is its own database (MySQL, MariaDB, SQL Server, Informix), schema (Postgres, CockroachDB and
YugabyteDB in database lfc, Db2 in LFCDB) or user (Oracle, in FREEPDB1). Its DDL is $DDL/<workload>-<dialect>.sql (shape.py
and ddl.py of lfc-source-statschema). Rows come from INSERT ... SELECT over the engine's own generator (fill.sh), or fill.py
for Db2, Informix and CockroachDB (PY: a python with their drivers). The build runs on local disk ($DATA/build-<master>),
tuned for loading (no fsync, no binary log, SIMPLE recovery); a clone starts with the image defaults. Admin passwords come
from $SECRETS/<engine>.env. Settings: DDL (/tmp/dg/ddl), DG (/tmp/dg), DATA (/var/lib/containers/lfc-data), OUT
(/mnt/jfs/masters), SECRETS (/root/lfc-src), JC (the JuiceFS cache), SESSIONS (4), MY_SESSIONS (2), DB2_SESSIONS (2),
CRDB_ROWS (50000), PY (python3), HP/IP/CRP (Db2's, Informix's, CockroachDB's local ports), IFX_DBSPACE_KB (1000000: the
Informix developer image refuses 30 GB, so its master is SF 0.1). It takes an hour or more: run it as a systemd unit.

export and import: MASTERS_URL (https://<account>.blob.core.windows.net/<container>), MASTERS_SAS_FILE (root-only, mode
600: the container's SAS, read from the file, never on a command line; import deletes it when it ends), MASTERS (all, a
comma list, or none for import's admin envs only). export: EXPORT_TMP (/var/lib/containers/masters-export), MASTER_ENGINES
(name=engine,... for a master without <master>.engine), MASTERS_BLOCK_MB (256) above MASTERS_PUT_BLOB_MAX_MB (5000); a
master whose blob has the same sha256 isn't uploaded again. import: STORAGE (juicefs, or local: ROOT /var/lib/containers,
SECRETS its .secrets); a download becomes the master only when its sha256 matches. One JSON line per master on stdout.
"""
from __future__ import annotations

import base64
import contextlib
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from . import engines
from .host import Fail
from .slots import read_env_file, setting

V = "2023-11-03"
NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def private_file(p: str) -> bool:
    """No group or other bits."""
    return os.stat(p).st_mode & 0o077 == 0


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


class Blob:
    """A Blob container through its SAS (in this process only: never on a command line)."""

    def __init__(self, url: str, sas_file: str):
        if not os.path.isfile(sas_file):
            raise Fail(f"no {sas_file}")
        if not private_file(sas_file):
            raise Fail(f"{sas_file}: chmod 600 it (no group or other access)")
        self.url = url.rstrip("/")
        self.sas = re.sub(r"\s", "", Path(sas_file).read_text()).lstrip("?")
        if not self.sas:
            raise Fail(f"{sas_file} is empty")

    def request(self, blob: str, method: str = "GET", query: str = "", data=None, headers=None, length=None):
        u = f"{self.url}/{blob}?{query + '&' if query else ''}{self.sas}"
        h = {"x-ms-version": V, **(headers or {})}
        if length is not None:
            h["Content-Length"] = str(length)
        return urllib.request.urlopen(urllib.request.Request(u, data=data, headers=h, method=method), timeout=300)

    def get(self, blob: str) -> bytes | None:
        """The blob, None when it doesn't exist."""
        try:
            with self.request(blob) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise Fail(f"{blob}: HTTP {e.code} (the SAS needs read)")
        except urllib.error.URLError as e:
            raise Fail(f"no connection to {self.url}: {e.reason}")

    def size(self, blob: str) -> int:
        with self.request(blob, "HEAD") as r:
            return int(r.headers.get("Content-Length", "-1"))

    def put(self, path: str, blob: str, content_type: str, block_mb: int, put_max_mb: int) -> None:
        """Put Blob up to put_max_mb, else Put Block per block_mb slice and Put Block List; then the stored size is the
        sent one."""
        size = os.path.getsize(path)
        if size <= put_max_mb << 20:
            def put_whole():     # the open file streams (urllib reads it in blocks); a retry opens it again
                with open(path, "rb") as f:
                    self.request(blob, "PUT", data=f, length=size,
                                 headers={"x-ms-blob-type": "BlockBlob", "Content-Type": content_type}).close()
            retry(put_whole, f"upload of {blob}")
        else:
            blk, ids = block_mb << 20, []
            with open(path, "rb") as f:
                for i in range((size + blk - 1) // blk):
                    f.seek(i * blk)
                    body = f.read(blk)
                    raw = base64.b64encode(b"%06d" % i).decode()      # the same length for every block
                    q = "comp=block&blockid=" + urllib.request.quote(raw, safe="")
                    retry(lambda q=q, body=body: self.request(blob, "PUT", q, body, length=len(body)).close(),
                          f"block {i} of {blob}")
                    ids.append(raw)
            xml = ('<?xml version="1.0" encoding="utf-8"?><BlockList>' +
                   "".join(f"<Latest>{i}</Latest>" for i in ids) + "</BlockList>").encode()
            retry(lambda: self.request(blob, "PUT", "comp=blocklist", xml, length=len(xml),
                                       headers={"x-ms-blob-content-type": content_type,
                                                "Content-Type": "application/xml"}).close(), f"block list of {blob}")
        got = self.size(blob)
        if got != size:
            raise Fail(f"{blob}: {got} bytes in storage, {size} sent")


def retry(fn, what: str, tries: int = 5) -> None:
    for k in range(1, tries + 1):
        try:
            return fn()
        except (urllib.error.URLError, OSError) as e:
            if k == tries:
                raise Fail(f"{what} failed: {e}")
            time.sleep(k * 5)


def manifest_of(blob: Blob, missing_ok: bool) -> dict:
    t = blob.get("manifest.json")
    if t is None:
        if not missing_ok:
            raise Fail("manifest.json: HTTP 404 (the SAS needs read)")
        return {"masters": {}, "envs": []}
    m = json.loads(t)
    if "masters" not in m or "envs" not in m:
        raise Fail("manifest.json has no masters or envs")
    return m


def jline(x) -> None:
    print(json.dumps(x, separators=(",", ":")), flush=True)


def sas_settings() -> tuple:
    url = os.environ.get("MASTERS_URL") or ""
    sas = os.environ.get("MASTERS_SAS_FILE") or ""
    if not url:
        raise Fail("MASTERS_URL=https://<account>.blob.core.windows.net/<container>")
    if not sas:
        raise Fail("MASTERS_SAS_FILE=<root-only file with the container SAS>")
    return url, sas


# ---- export
def export(names: str = "") -> int:
    root, secrets = setting("ROOT", "/mnt/jfs"), setting("SECRETS", "/root/lfc-src")
    url, sas = sas_settings()
    blob = Blob(url, sas)
    block_mb = int(os.environ.get("MASTERS_BLOCK_MB") or 256)
    put_max = int(os.environ.get("MASTERS_PUT_BLOB_MAX_MB") or 5000)
    tmp_root = os.environ.get("EXPORT_TMP") or "/var/lib/containers/masters-export"
    os.makedirs(tmp_root, mode=0o700, exist_ok=True)
    man = manifest_of(blob, missing_ok=True)
    want = names or os.environ.get("MASTERS") or "all"
    if want == "all":
        todo = sorted(p.name for p in Path(root, "masters").glob("*") if p.is_dir())
    else:
        todo = [n for n in want.split(",") if n]
    if not todo:
        raise Fail(f"no masters in {root}/masters")
    with tempfile.TemporaryDirectory(prefix="run.", dir=tmp_root) as t:
        for n in todo:
            export_one(blob, man, root, secrets, n, t, block_mb, put_max)
    return 0


def engine_of(root: str, n: str) -> str:
    """The engine beside the master (<master>.engine, which build writes), else MASTER_ENGINES."""
    e = ""
    with contextlib.suppress(OSError):
        e = Path(root, "masters", f"{n}.engine").read_text().strip()
    if not e:
        pairs = dict(x.split("=", 1) for x in (os.environ.get("MASTER_ENGINES") or "").split(",") if "=" in x)
        e = pairs.get(n, "")
    if e not in engines.ENGINES:
        raise Fail(f"{n}: no engine (write {root}/masters/{n}.engine, or MASTER_ENGINES={n}=<engine>)")
    return e


def export_one(blob: Blob, man: dict, root: str, secrets: str, n: str, t: str, block_mb: int, put_max: int) -> None:
    if not NAME.match(n):
        raise Fail(f"bad master name: {n}")
    src = Path(root, "masters", n)
    if not src.is_dir():
        raise Fail(f"no master {n} in {root}/masters")
    eng = engine_of(root, n)
    need = sum(f.stat().st_size for f in src.rglob("*") if f.is_file() and not f.is_symlink())
    free = shutil.disk_usage(t).free
    if free < need:
        raise Fail(f"no room for the archive in {t}: {free} bytes free, {n} is {need} (set EXPORT_TMP to a bigger disk)")
    a = os.path.join(t, f"{n}.tar.zst")
    with open(a, "wb") as out:
        tar = subprocess.Popen(["tar", "--numeric-owner", "-C", f"{root}/masters", "-cf", "-", n], stdout=subprocess.PIPE)
        z = subprocess.run(["zstd", "-T0", "-q", "-c"], stdin=tar.stdout, stdout=out, check=False)
        tar.stdout.close()
        if tar.wait() or z.returncode:
            raise Fail(f"tar or zstd failed on {n}")
    sha, size = sha256(a), os.path.getsize(a)
    if (man["masters"].get(n) or {}).get("sha256") == sha:
        state = "skipped"
        print(f"masters-export: {n}: the blob has this sha256 already", file=sys.stderr)
    else:
        print(f"masters-export: {n}: uploading {size} bytes", file=sys.stderr)
        blob.put(a, f"{n}.tar.zst", "application/octet-stream", block_mb, put_max)
        state = "uploaded"
        man["masters"][n] = {"engine": eng, "bytes": size, "sha256": sha,
                             "files": sum(1 for f in src.rglob("*") if f.is_file() and not f.is_symlink())}
    env = Path(secrets, f"{eng}.env")          # the admin env the clones get (a secret: the private container only)
    if not env.is_file():
        raise Fail(f"no {env}: the admin env of the {eng} masters")
    esha = sha256(str(env))
    if next((x["sha256"] for x in man["envs"] if x["engine"] == eng), None) != esha:
        blob.put(str(env), f"{eng}.env", "text/plain", block_mb, put_max)
        man["envs"] = [x for x in man["envs"] if x["engine"] != eng] + [
            {"engine": eng, "file": f"{eng}.env", "bytes": env.stat().st_size, "sha256": esha}]
    man["created"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    mf = os.path.join(t, "manifest.json")
    Path(mf).write_text(json.dumps(man, indent=2) + "\n")
    blob.put(mf, "manifest.json", "application/json", block_mb, put_max)
    os.unlink(a)
    jline({"master": n, "engine": eng, "bytes": size, "sha256": sha, "state": state})


# ---- import
def import_(names: str = "") -> int:
    url, sas = sas_settings()
    try:
        return _import(url, sas, names or os.environ.get("MASTERS") or "all")
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(sas)                       # the SAS goes when this ends


def _import(url: str, sas: str, want: str) -> int:
    blob = Blob(url, sas)
    local = os.environ.get("STORAGE") == "local"
    root = setting("ROOT", "/var/lib/containers" if local else "/mnt/jfs")
    secrets = setting("SECRETS", "/var/lib/containers/.secrets" if local else "/root/lfc-src")
    if not local and subprocess.run(["findmnt", "-t", "fuse.juicefs", root], capture_output=True,
                                    check=False).returncode:
        raise Fail(f"{root} isn't mounted (lfc-jfs.service): no masters are taken yet")
    os.makedirs(f"{root}/masters", exist_ok=True)
    os.makedirs(secrets, mode=0o700, exist_ok=True)
    os.chmod(secrets, 0o700)
    man = manifest_of(blob, missing_ok=False)
    names = [] if want == "none" else sorted(man["masters"]) if want == "all" else [n for n in want.split(",") if n]
    for n in names:
        if not NAME.match(n):
            raise Fail(f"bad master name: {n}")
        if n not in man["masters"]:
            raise Fail(f"{n} isn't in the manifest")
    if want != "none" and not names:
        raise Fail("no masters in the manifest")
    # every engine's admin env, with or without masters; a different file where one goes would make a clone's password
    # differ from its master's: stop before anything is written
    envs = {}
    for x in man["envs"]:
        envs.setdefault(x["engine"], x["sha256"])
    for e, want_sha in envs.items():
        f = Path(secrets, f"{e}.env")
        if f.exists() and sha256(str(f)) != want_sha:
            raise Fail(f"{f} exists with other content: move it aside first")
    for e, want_sha in envs.items():
        f = Path(secrets, f"{e}.env")
        if f.exists():
            continue
        body = blob.get(f"{e}.env")
        if body is None:
            raise Fail(f"{e}.env: download failed")
        if hashlib.sha256(body).hexdigest() != want_sha:
            raise Fail(f"{e}.env: sha256 doesn't match the manifest")
        fd = os.open(f, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as out:
            out.write(body)
    for n in names:
        import_one(blob, man["masters"][n], root, n)
    return 0


def import_one(blob: Blob, rec: dict, root: str, n: str) -> None:
    """The blob streams through sha256 and zstd into a staging folder, which becomes the master only when the sum
    matches."""
    d, e = Path(root, "masters", n), rec["engine"]
    if d.is_dir():
        return jline({"master": n, "engine": e, "state": "exists"})
    stg = Path(root, "masters", f".incoming-{n}")
    shutil.rmtree(stg, ignore_errors=True)
    stg.mkdir()
    h, ok = hashlib.sha256(), False
    unz = subprocess.Popen(["zstd", "-dc", "-q"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    tar = subprocess.Popen(["tar", "-x", "--numeric-owner", "-C", str(stg)], stdin=unz.stdout)
    unz.stdout.close()
    try:
        with blob.request(f"{n}.tar.zst") as r:
            for b in iter(lambda: r.read(1 << 20), b""):
                h.update(b)
                unz.stdin.write(b)
        ok = True
    except (urllib.error.URLError, OSError):
        ok = False
    finally:
        with contextlib.suppress(BrokenPipeError):
            unz.stdin.close()
    rcs = (unz.wait(), tar.wait())
    got = h.hexdigest()
    if not ok or any(rcs) or got != rec["sha256"] or not (stg / n).is_dir():
        shutil.rmtree(stg, ignore_errors=True)
        raise Fail(f"{n}: the download didn't verify (sha256 {got}, manifest {rec['sha256']}, statuses {list(rcs)}): "
                   "the partial master is removed")
    os.replace(stg / n, d)
    shutil.rmtree(stg, ignore_errors=True)
    Path(root, "masters", f"{n}.engine").write_text(e + "\n")
    jline({"master": n, "engine": e, "state": "imported"})


# ---- build
WORKLOADS = ("tpcb", "tpcc", "tpch", "tpcds", "tpce", "tpcdi")
NOISE = re.compile(r"^\(?[0-9]+ rows? affected\)?$|^$")
DDL_ERR = re.compile(r"^(Msg [0-9]+, Level 1[1-9]|ORA-[0-9]+|SP2-[0-9]+)", re.MULTILINE)


def staging(cache: str) -> bool:
    """A file still in JuiceFS's write-back staging (not yet uploaded)."""
    return any("rawstaging" in d and files for d, _, files in os.walk(cache))


class Build:
    """One master's build: start the engine on local disk, load each workload, statistics and a clean stop, then the
    copy into OUT."""

    def __init__(self, eng: str, name: str, workloads):
        if eng not in engines.ENGINES:
            raise Fail("engine: postgres, mysql, mariadb, sqlserver, oracle, cockroachdb, yugabytedb, db2 or informix", 2)
        g = lambda k, d: os.environ.get(k) or d
        self.e, self.name, self.wl = eng, name, list(workloads) or list(WORKLOADS)
        self.ddl, self.dg = g("DDL", "/tmp/dg/ddl"), g("DG", "/tmp/dg")
        self.out, self.secrets = g("OUT", "/mnt/jfs/masters"), g("SECRETS", "/root/lfc-src")
        self.jc, self.sessions, self.py = g("JC", "/var/lib/containers/jfs/cache"), g("SESSIONS", "4"), g("PY", "python3")
        self.hp, self.ip, self.crp = g("HP", "50123"), g("IP", "9188"), g("CRP", "26299")
        self.m = f"{g('DATA', '/var/lib/containers/lfc-data')}/build-{name}"
        self.c = f"build-{name}"
        self.env = f"{self.secrets}/{eng}.env"
        self.labels = ["--label", f"lfc.engine={eng}", "--label", "lfc.purpose=tpc-master", "--label", f"lfc.master={name}"]

    # helpers
    def sh(self, script: str, input: str | None = None, env: dict | None = None, out=None) -> int:
        """bash for the fill planners and the clients they take as commands."""
        return subprocess.run(["bash", "-c", script], input=input, text=True, env={**os.environ, **(env or {})},
                              stdout=out, stderr=subprocess.STDOUT if out else None, check=False).returncode

    def podman(self, *args, input: str | None = None, quiet: bool = True, silent: bool = False) -> int:
        """podman's exit status; quiet: no stdout; silent: no stderr either (a probe that fails until it's ready)."""
        return subprocess.run(["podman", *args], input=input, text=True, check=False,
                              stdout=subprocess.DEVNULL if quiet or silent else None,
                              stderr=subprocess.DEVNULL if silent else None).returncode

    def running(self) -> bool:
        r = subprocess.run(["podman", "container", "inspect", "-f", "{{.State.Running}}", self.c], capture_output=True,
                           text=True, check=False)
        return r.stdout.strip() == "true"

    def wait_for(self, check) -> None:
        """Until check() passes, while the container runs (else show why it stopped)."""
        while not check():
            if not self.running():
                tail = subprocess.run(["podman", "logs", "--tail", "20", self.c], capture_output=True, text=True,
                                      check=False)
                raise Fail(f"{self.c} stopped:\n{tail.stdout}{tail.stderr}")
            time.sleep(3)

    def logs_have(self, line: str) -> bool:
        r = subprocess.run(["podman", "logs", self.c], capture_output=True, text=True, check=False)
        return line in r.stdout + r.stderr

    def run_db(self, *opts_and_image) -> None:
        if self.podman("run", "-d", "--name", self.c, *self.labels, *opts_and_image):
            raise Fail(f"podman run {self.c} failed")

    def password_file(self, key: str, w: str) -> str:
        p = os.path.join(w, "pw")
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(read_env_file(self.env).get(key, "") + "\n")
        return p

    def ddl_ok(self, w: str, out: str) -> bool:
        m = DDL_ERR.findall(out)
        if m:
            print("\n".join(m) + f"\nDDL for {w} failed (above)")
        return not m

    def script(self, w: str, name: str, body: str) -> str:
        p = os.path.join(w, name)
        Path(p).write_text(body)
        os.chmod(p, 0o700)
        return p

    # the engines
    def postgres(self, w):
        img = engines.get("postgres").image
        self.run_db("--shm-size", "4g", "--env-file", self.env, "-v", f"{self.m}:/var/lib/postgresql/data:Z", img,
                    "-c", "shared_buffers=4GB", "-c", "maintenance_work_mem=1GB", "-c", "max_wal_size=16GB",
                    "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off")
        self.wait_for(lambda: self.podman("exec", self.c, "pg_isready", "-h", "127.0.0.1", "-q", silent=True) == 0)
        time.sleep(2)
        pg = f"podman exec -i {self.c} psql -X -U postgres -d postgres -q -v ON_ERROR_STOP=1"

        def load(x, log):
            return (self.sh(pg, f"CREATE SCHEMA {x}; SET search_path = {x};\n" + Path(self.ddl, f"{x}-postgres.sql")
                            .read_text(), out=log) == 0
                    and self.sh(f"bash {self.dg}/fill.sh pg {x}", env={"PSQL": f"podman exec -i {self.c} psql -U postgres "
                                "-d postgres", "DIR": f"{self.dg}/postgres", "SRC": "series", "SESSIONS": self.sessions},
                                out=log) == 0)

        def finish():
            # after a clean stop (no replication slots), pg_resetwal drops the load's WAL to one segment
            return (self.sh(f'{pg} -c "VACUUM ANALYZE" && {pg} -c CHECKPOINT') == 0 and
                    self.podman("stop", "-t", "300", self.c) == 0 and
                    self.podman("run", "--rm", "--user", "postgres", "-v", f"{self.m}:/var/lib/postgresql/data:Z", img,
                                "pg_resetwal", "-D", "/var/lib/postgresql/data", quiet=False) == 0)
        return load, finish

    def pg_protocol(self, w):
        """CockroachDB and YugabyteDB: the Postgres planners through psql, in a sidecar sharing the server's network."""
        crdb = self.e == "cockroachdb"
        eng = engines.get(self.e)
        p, u, admin, ddld = ("26257", "root", "defaultdb", "cockroachdb") if crdb else ("5433", "yugabyte", "yugabyte",
                                                                                         "postgres")
        if crdb:
            empty = os.path.join(w, "empty")
            Path(empty).write_text("")             # insecure: fill.py reads the (empty) password file
            self.run_db("-v", f"{self.m}:/cockroach/cockroach-data:Z", "-p", f"127.0.0.1:{self.crp}:26257", eng.image,
                        *eng.args)
        else:
            subprocess.run(["chown", "10001:100", self.m], check=False)
            self.run_db("-v", f"{self.m}:/yb:Z", eng.image, *eng.args)
        pgimg = engines.get("postgres").image
        pgcmd = f"podman run --rm -i --network container:{self.c} {pgimg} psql -X -q -h 127.0.0.1 -p {p} -U {u}"
        self.wait_for(lambda: self.sh(f"{pgcmd} -At -d {admin} -c 'SELECT 1' >/dev/null 2>&1") == 0)
        time.sleep(2)
        if self.sh(f'{pgcmd} -d {admin} -v ON_ERROR_STOP=1 -c "CREATE DATABASE lfc" >/dev/null'):
            raise Fail("CREATE DATABASE lfc failed")
        pg = f"{pgcmd} -d lfc -v ON_ERROR_STOP=1"

        def load(x, log):
            ddl = f"CREATE SCHEMA {x}; SET search_path = {x};\n" + Path(self.ddl, f"{x}-{ddld}.sql").read_text()
            if self.sh(pg, ddl, out=log):
                return False
            if crdb:     # CockroachDB refuses the planners' pg_temp functions: fill.py builds the same INSERTs
                return self.sh(f"{self.py} {self.dg}/python/fill.py pg {x} --database lfc --host 127.0.0.1 --port "
                               f"{self.crp} --user root --password-file {empty} --sessions {self.sessions} --rows "
                               f"{os.environ.get('CRDB_ROWS') or 50000}", out=log) == 0
            return self.sh(f"bash {self.dg}/fill.sh pg {x}", env={"PSQL": f"{pgcmd} -d lfc", "DIR": f"{self.dg}/postgres",
                                                                  "SRC": "series", "SESSIONS": self.sessions},
                           out=log) == 0

        def finish():
            if crdb:
                self.podman("exec", self.c, "/cockroach/cockroach", "node", "drain", "--self", "--insecure", "--shutdown")
            else:
                self.sh(f'{pg} -c "ANALYZE"')
            return self.podman("stop", "-t", "120", self.c) == 0
        return load, finish

    def mysql(self, w):
        maria = self.e == "mariadb"
        cli, pwv = ("mariadb", "MARIADB_ROOT_PASSWORD") if maria else ("mysql", "MYSQL_ROOT_PASSWORD")
        args = ["--skip-log-bin", "--innodb-buffer-pool-size=4G", "--innodb-flush-log-at-trx-commit=0",
                "--innodb-log-file-size=2G" if maria else "--innodb-redo-log-capacity=2G"]
        self.run_db("--env-file", self.env, "-v", f"{self.m}:/var/lib/mysql:Z", engines.get(self.e).image, *args)
        self.wait_for(lambda: self.podman("exec", self.c, "sh", "-c", f"MYSQL_PWD=${pwv} {cli} -uroot -e 'SELECT 1'",
                                          silent=True) == 0)
        self.podman("exec", self.c, "sh", "-c",
                    f'umask 077; printf "[client]\\nuser=root\\npassword=%s\\n" "${pwv}" > /root/.my.cnf')   # not on argv
        my = f"podman exec -i {self.c} {cli}"
        seed = ["CREATE DATABASE IF NOT EXISTS lfc_seed; CREATE TABLE lfc_seed.seed (i bigint PRIMARY KEY);",   # 1..2^20
                "INSERT INTO lfc_seed.seed (i) VALUES " + ",".join(f"({i})" for i in range(1, 1025)) + ";"]
        seed += ["INSERT INTO lfc_seed.seed (i) SELECT s.i + b.m FROM lfc_seed.seed s, (SELECT MAX(i) AS m FROM "
                 "lfc_seed.seed) b;"] * 10
        seed += ["CREATE TABLE lfc_seed.seed_r (i bigint PRIMARY KEY, r bigint NOT NULL);",   # r: shuffled keys
                 "INSERT INTO lfc_seed.seed_r SELECT i, ROW_NUMBER() OVER (ORDER BY RAND()) FROM lfc_seed.seed;",
                 "DROP TABLE lfc_seed.seed; RENAME TABLE lfc_seed.seed_r TO lfc_seed.seed;"]
        if self.sh(my, "\n".join(seed) + "\n"):
            raise Fail("the seed table failed")

        def load(x, log):
            return (self.sh(my, f"CREATE DATABASE {x}; USE {x};\n" + Path(self.ddl, f"{x}-mysql.sql").read_text(),
                            out=log) == 0
                    and self.sh(f"bash {self.dg}/fill.sh my {x}", env={"MYSQL": my, "DIR": f"{self.dg}/mysql",
                                "SESSIONS": os.environ.get("MY_SESSIONS") or "2"}, out=log) == 0)

        def finish():
            for x in self.wl:
                self.sh(f"{my} -N -e \"SELECT CONCAT('ANALYZE TABLE \\`', table_schema, '\\`.\\`', table_name, '\\`;') "
                        f"FROM information_schema.tables WHERE table_schema = '{x}'\" | {my} > /dev/null")
            return self.sh(f'{my} -e "DROP TABLE lfc_seed.seed"') == 0 and self.podman("stop", "-t", "300", self.c) == 0
        return load, finish

    def sqlserver(self, w):
        subprocess.run(["chown", "10001:0", self.m], check=False)
        self.run_db("--memory", "6g", "--env-file", self.env, "-e", "MSSQL_MEMORY_LIMIT_MB=5120", "-v",
                    f"{self.m}:/var/opt/mssql:Z", engines.get("sqlserver").image)
        # sqlcmd gets a file (-i): it splits long stdin lines
        sq = self.script(w, "sq", f"#!/bin/bash\nexec podman exec -i {self.c} bash -c 'f=$(mktemp); cat > $f; "
                         "SQLCMDPASSWORD=$MSSQL_SA_PASSWORD /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -d \"$0\" "
                         "-y 0 -i $f; r=$?; rm -f $f; exit $r' \"${1:-master}\"\n")
        self.wait_for(lambda: self.sh(f"{sq} >/dev/null 2>&1", "SELECT 1\n") == 0)

        def load(x, log):
            if self.sh(f"{sq} > /dev/null", f"CREATE DATABASE [{x}]; ALTER DATABASE [{x}] SET RECOVERY SIMPLE;\n"):
                return False
            r = subprocess.run([sq, x], input=Path(self.ddl, f"{x}-tsql.sql").read_text(), capture_output=True,
                               text=True, check=False)
            if r.returncode or not self.ddl_ok(x, r.stdout):
                return False
            sqx = self.script(w, f"sq-{x}", f"#!/bin/bash\nexec {sq} {x}\n")
            return self.sh(f"bash {self.dg}/fill.sh ms dbo", env={"MSSQL": sqx, "DIR": f"{self.dg}/sqlserver",
                                                                  "SESSIONS": self.sessions}, out=log) == 0

        def finish():
            for x in self.wl:
                if self.sh(f"{sq} {x} > /dev/null", "EXEC sp_updatestats;\n"):
                    return False
            self.sh(f"{sq} > /dev/null", "SHUTDOWN;\n")
            return self.podman("stop", "-t", "120", self.c) == 0
        return load, finish

    def oracle(self, w):
        subprocess.run(["chown", "54321:54321", self.m], check=False)
        self.run_db("--memory", "4g", "--shm-size", "2g", "--env-file", self.env, "-v", f"{self.m}:/opt/oracle/oradata:Z",
                    engines.get("oracle").image)
        sq = self.script(w, "sq", f"#!/bin/bash\nexec podman exec -i {self.c} bash -c '{{ echo \"SET FEEDBACK OFF\"; "
                         "echo \"ALTER SESSION SET CONTAINER = FREEPDB1;\"; cat; } | sqlplus -s / as sysdba'\n")
        self.wait_for(lambda: self.logs_have(engines.READY_LOG["oracle"]))

        def load(x, log):
            s = x.upper()
            r = subprocess.run([sq], input=f"SET DEFINE OFF\nCREATE USER {s} NO AUTHENTICATION QUOTA UNLIMITED ON USERS "
                               f"DEFAULT TABLESPACE USERS;\nALTER SESSION SET CURRENT_SCHEMA = {s};\n" +
                               Path(self.ddl, f"{x}-oracle.sql").read_text(), capture_output=True, text=True, check=False)
            if not self.ddl_ok(x, r.stdout):
                return False
            return self.sh(f"bash {self.dg}/fill.sh ora {s}", env={"ORACLE": sq, "DIR": f"{self.dg}/oracle",
                                                                   "SESSIONS": self.sessions}, out=log) == 0

        def finish():
            for x in self.wl:
                self.sh(sq, f"EXEC DBMS_STATS.GATHER_SCHEMA_STATS('{x.upper()}')\n")
            return self.podman("stop", "-t", "120", self.c) == 0
        return load, finish

    def db2(self, w):
        """LFCDB made after the image's setup with 4 KB-aligned I/O (a 4096-byte-sector disk refuses Db2's 512-byte
        direct-I/O writes: SQL0293N); fill.py fills each schema."""
        pw = self.password_file("DB2INST1_PASSWORD", w)
        self.run_db("--privileged", "--env-file", self.env, "-e", "DBNAME=", "-e", "PERSISTENT_HOME=true", "-v",
                    f"{self.m}:/database:Z", "-p", f"127.0.0.1:{self.hp}:50000", engines.get("db2").image)
        pyc = f"{self.py} {self.dg}/python/sq.py db2 --host 127.0.0.1 --port {self.hp} --user db2inst1 --database LFCDB " \
              f"--password-file {pw}"
        self.wait_for(lambda: self.logs_have(engines.READY_LOG["db2"]))
        if self.podman("exec", self.c, "su", "-", "db2inst1", "-c", "db2set DB2_4K_DEVICE_SUPPORT=ON && { db2stop force; "
                       "db2start; } >/dev/null 2>&1; db2 create database LFCDB"):
            raise Fail("CREATE DATABASE LFCDB failed")
        self.wait_for(lambda: self.sh(f"{pyc} >/dev/null 2>&1", "SELECT 1 FROM SYSIBM.SYSDUMMY1;\n") == 0)
        if self.sh(f"{pyc} > /dev/null", "CALL SYSPROC.ADMIN_CMD('UPDATE DB CFG FOR LFCDB USING PCKCACHESZ 16384');\n"
                   "CALL SYSPROC.ADMIN_CMD('UPDATE DB CFG FOR LFCDB USING LOGSECOND 200');\n"):
            raise Fail("the Db2 settings failed")

        def load(x, log):
            s = x.upper()
            return (self.sh(f"{pyc} --schema {s}", f"CREATE SCHEMA {s};\n" + Path(self.ddl, f"{x}-db2.sql").read_text(),
                            out=log) == 0
                    and self.sh(f"{self.py} {self.dg}/python/fill.py db2 {x} --database LFCDB --host 127.0.0.1 --port "
                                f"{self.hp} --user db2inst1 --password-file {pw} --sessions "
                                f"{os.environ.get('DB2_SESSIONS') or 2}", out=log) == 0)

        def finish():
            self.podman("exec", self.c, "su", "-", "db2inst1", "-c", "db2 force applications all; db2stop force")
            return self.podman("stop", "-t", "120", self.c) == 0
        return load, finish

    def informix(self, w):
        """One database per workload in a dbspace of its own (the image's root dbspace is 1.6 GB)."""
        os.makedirs(f"{self.m}/cfg", exist_ok=True)
        pw = self.password_file("INFORMIX_PASSWORD", w)
        self.run_db("--env-file", self.env, "-v", f"{self.m}:/opt/ibm/data:U,Z", "-v", f"{self.m}/cfg:/opt/ibm/config:U,Z",
                    "-p", f"127.0.0.1:{self.ip}:9088", engines.get("informix").image)
        pys = f"{self.py} {self.dg}/python/sq.py ifx --host 127.0.0.1 --port {self.ip} --user informix --password-file {pw}"
        self.wait_for(lambda: self.sh(f"{pys} --database sysmaster >/dev/null 2>&1",
                                      "SELECT 1 FROM sysmaster:sysdual;\n") == 0)
        # the OS password the server checks (PAM), then the dbspace (its chunk file owned by informix, mode 660)
        if self.podman("exec", "-i", "-u", "root", self.c, "chpasswd",
                       input=f"informix:{read_env_file(self.env).get('INFORMIX_PASSWORD', '')}\n"):
            raise Fail("the Informix admin's OS password failed")
        chunk = "/opt/ibm/data/spaces/datadbs.000"
        if self.podman("exec", "-u", "root", self.c, "bash", "-c", f"touch {chunk} && chown informix:informix {chunk} && "
                       f"chmod 660 {chunk}"):
            raise Fail("the dbspace's chunk file failed")
        r = subprocess.run(["podman", "exec", "-u", "informix", self.c, "bash", "-lc", f"onspaces -c -d datadbs -p {chunk} "
                            f"-o 0 -s {os.environ.get('IFX_DBSPACE_KB') or 1000000}"], capture_output=True, text=True,
                           check=False)
        if r.returncode:
            raise Fail(f"onspaces failed:\n{r.stdout}{r.stderr}")

        def load(x, log):
            return (self.sh(f"{pys} --database sysmaster", f"CREATE DATABASE {x} IN datadbs WITH LOG;\n", out=log) == 0
                    and self.sh(f"{pys} --database {x}", Path(self.ddl, f"{x}-informix.sql").read_text(), out=log) == 0
                    and self.sh(f"{self.py} {self.dg}/python/fill.py ifx {x} --database {x} --host 127.0.0.1 --port "
                                f"{self.ip} --user informix --password-file {pw} --sessions 1", out=log) == 0)

        def finish():
            self.podman("exec", "-u", "informix", self.c, "bash", "-lc", "onmode -ky")
            return self.podman("stop", "-t", "120", self.c) == 0
        return load, finish

    def run(self) -> int:
        if not os.path.isfile(self.env):
            raise Fail(f"missing {self.env}", 2)
        if os.path.exists(f"{self.out}/{self.name}"):
            raise Fail(f"{self.out}/{self.name} exists: remove it first", 2)
        self.podman("rm", "-f", "--depend", self.c)       # --depend: an earlier run's psql sidecar blocks a plain rm -f
        shutil.rmtree(self.m, ignore_errors=True)
        os.makedirs(self.m)
        start = {"postgres": self.postgres, "cockroachdb": self.pg_protocol, "yugabytedb": self.pg_protocol,
                 "mysql": self.mysql, "mariadb": self.mysql, "sqlserver": self.sqlserver, "oracle": self.oracle,
                 "db2": self.db2, "informix": self.informix}[self.e]
        with tempfile.TemporaryDirectory() as w:
            load, finish = start(w)
            for x in self.wl:
                print(f"== {x}", flush=True)
                logp = os.path.join(w, "load.out")
                t0 = time.time()
                with open(logp, "w") as log:
                    ok = load(x, log)
                print(f"  [{int(time.time() - t0)} s] load {x}")
                lines = [ln for ln in Path(logp).read_text(errors="replace").splitlines() if not NOISE.match(ln)]
                print("\n".join(lines[-40:]), flush=True)
                if not ok:
                    raise Fail(f"{x} failed")
            print("== statistics, clean stop", flush=True)
            if not finish():
                raise Fail(f"statistics or the clean stop failed: not copied; the build is still in {self.m} "
                           f"(container {self.c})")
        self.podman("rm", self.c)
        du = subprocess.run(["du", "-sh", self.m], capture_output=True, text=True, check=False).stdout.split()
        print(f"master on disk: {du[0] if du else '?'}, files {sum(1 for f in Path(self.m).rglob('*') if f.is_file())}")
        # into JuiceFS (then wait until its write-back staging is uploaded), or a reflink copy on the same XFS
        if subprocess.run(["cp", "-a", "--reflink=auto", self.m, f"{self.out}/{self.name}"], check=False).returncode:
            raise Fail(f"copy to {self.out}/{self.name} failed: the build is still in {self.m}")
        Path(self.out, f"{self.name}.engine").write_text(self.e + "\n")   # export reads it
        while staging(self.jc):
            time.sleep(2)
        info = subprocess.run(["juicefs", "info", "-r", f"{self.out}/{self.name}"], capture_output=True, text=True,
                              check=False).stdout
        m = re.search(r"size:\s*(\S+)\s*(\S*)", info)
        print(f"copied to {self.out}/{self.name}: {(m.group(1) + m.group(2)) if m else ''}")
        shutil.rmtree(self.m, ignore_errors=True)
        print(f"MASTER DONE {self.name}")
        return 0


def main(argv: list[str]) -> int:
    try:
        if argv[:1] == ["export"]:
            return export()
        if argv[:1] == ["import"]:
            return import_()
        if argv[:1] == ["build"] and len(argv) >= 3:
            return Build(argv[1], argv[2], argv[3:]).run()
        print(__doc__.strip(), file=sys.stderr)
        return 2
    except Fail as x:
        if x.msg:
            print(f"masters: {x.msg}", file=sys.stderr)
        return x.rc
