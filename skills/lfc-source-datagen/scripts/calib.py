#!/usr/bin/env python3
"""calib.py <pg|my> <size|sessions>: rows/s for statement sizes, then for session counts, sampling vmstat (run queue r,
iowait) and /proc/diskstats (await ms, util %) for the data disk during each run. Linux, on the database's host (it reads
/proc and runs vmstat). The database runs in a container: the clients are podman exec commands (a subprocess, as before:
the SQL planner files cast.sql and plan-insert.sql are the planner under test, so they run, not fill.py's planner).
    DEV=vda DIR=scripts/postgres python3 calib.py pg size      # statement sizes, TOTAL rows each (default 4,000,000)
    DIR=scripts/mysql python3 calib.py my sessions             # 1, 2, 4, 8 sessions, PER rows each (default 2,000,000)
Env: DEV (the data disk, default vda), DIR (the planner files' folder, default .), SIZES, TOTAL (size), B and PER (sessions).
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

USAGE = "usage: calib.py <pg|my> <size|sessions>"


def _read(path):
    with open(path) as f:
        return f.read()


class Client:
    """The planner's client for one engine: the container's SQL client, and the calls the calibration makes."""

    def __init__(self, engine, folder):
        self.pg = engine == "pg"
        self.dir = folder
        self.sql = (["podman", "exec", "-i", "dg-pg", "psql", "-U", "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1"] if self.pg
                    else ["podman", "exec", "-i", "dg-my", "mysql", "-uroot", "-N", "-B", "-r", "cal"])

    def _run(self, text, args=()):
        p = subprocess.run(self.sql + list(args), input=text, capture_output=True, text=True, check=False)
        return p.stdout

    def plan(self, rows, table):
        """The statements that insert `rows` rows into cal.<table>, the lines that name the table (grep's)."""
        if self.pg:
            text = _read(os.path.join(self.dir, "cast.sql")) + _read(os.path.join(self.dir, "plan-insert.sql"))
            out = self._run(text, ["-v", "schema=cal", "-v", f"rows={rows}", "-v", "src=seed", "-v", "order=random",
                                   "-v", "data=scramble", "-v", "plain_every=4"])
            return "".join(l + "\n" for l in out.splitlines() if f"cal.{table} " in l)
        head = (f"SET @db='cal', @rows={rows}, @src='seed', @order='random', @data='scramble', @plain_every=4, @mode='append';\n")
        out = self._run(head + _read(os.path.join(self.dir, "cast.sql")) + _read(os.path.join(self.dir, "plan-insert.sql")))
        return "".join(l + "\n" for l in out.splitlines() if f"`cal`.`{table}`" in l)

    def trunc(self, table):
        self._run(f"TRUNCATE cal.{table}" if self.pg else f"TRUNCATE TABLE cal.{table}")

    def exec_file(self, path):
        with open(path) as f:
            subprocess.run(self.sql, stdin=f, stdout=subprocess.DEVNULL, check=False)


class Monitor:
    """vmstat 1 and the data disk's /proc/diskstats line, once a second, until stop()."""

    def __init__(self, dev):
        self.dev = dev
        self.tmp = tempfile.mkdtemp(prefix="calib-")
        self.vm_path = os.path.join(self.tmp, "vm.out")
        self.ds = []
        self.stopped = threading.Event()

    def start(self):
        self.vm_file = open(self.vm_path, "w")  # noqa: SIM115 (closed by stop)
        self.vm = subprocess.Popen(["vmstat", "1"], stdout=self.vm_file)
        self.thread = threading.Thread(target=self._disk, daemon=True)
        self.thread.start()

    def _disk(self):
        while not self.stopped.is_set():
            try:
                with open("/proc/diskstats") as f:
                    for line in f:
                        fields = line.split()
                        if len(fields) > 2 and fields[2] == self.dev:
                            self.ds.append(fields)
            except OSError:
                pass
            self.stopped.wait(1)

    def stop(self):
        """(run queue, iowait, disk) for this run (see summary)."""
        self.vm.kill(); self.vm.wait()
        self.stopped.set(); self.thread.join()
        self.vm_file.close()
        return summary(_read(self.vm_path).splitlines(), self.ds)


def summary(vm_lines, ds):
    """(run queue, iowait, disk) for one run, as the shell version's awk computed them: vmstat's first 3 lines are the header and
    the average since boot; the disk's await is the ms per I/O between two samples, util the share of the second it was busy."""
    rq = [float(l.split()[0]) for l in vm_lines[3:] if l.split() and l.split()[0].isdigit()]
    r = f"r avg {sum(rq) / len(rq):.1f} max {int(max(rq))}" if rq else "r avg 0.0 max 0"
    wa_vals = [float(l.split()[15]) for l in vm_lines[3:] if len(l.split()) > 15]
    wa = f"iowait {sum(wa_vals) / len(wa_vals):.0f}%" if wa_vals else "iowait 0%"
    awaits, utils, prev = [], [], None
    for f in ds:
        ios, ms, io_ticks = int(f[3]) + int(f[7]), int(f[6]) + int(f[10]), int(f[12])
        if prev is not None and ios > prev[0]:
            a = (ms - prev[1]) / (ios - prev[0])
            awaits.append(a)
            utils.append((io_ticks - prev[2]) / 10)
        prev = (ios, ms, io_ticks)
    io = (f"await avg {sum(awaits) / len(awaits):.1f} max {max(awaits):.1f} ms, util {sum(utils) / len(utils):.0f}%"
          if awaits else "")
    return r, wa, io


def _line(rows, secs, mon, cpus):
    r, wa, io = mon
    return f"{rows / secs:.0f} rows/s ({secs:.1f} s); {r} (cpus {cpus}), {wa}" + (f", {io}" if io else "")


def size_phase(c, mon_dev, cpus, total, sizes, tmp):
    for b in sizes:
        c.trunc("t1")
        one = os.path.join(tmp, "one.sql")
        with open(one, "w") as f:
            f.write(c.plan(b, "t1"))
        n = total // b
        m = Monitor(mon_dev); m.start()
        s = time.time()
        for _ in range(n):
            c.exec_file(one)
        e = time.time()
        print(f"rows/statement {b} x {n}: {_line(total, e - s, m.stop(), cpus)}", flush=True)
    c.trunc("t1")


def session_phase(c, mon_dev, cpus, b, per, tmp):
    for p in (1, 2, 4, 8):
        files = []
        for k in range(1, p + 1):
            c.trunc(f"t{k}")
            path = os.path.join(tmp, f"s{k}.sql")
            with open(path, "w") as f:
                f.write(c.plan(b, f"t{k}"))
            files.append(path)
        m = Monitor(mon_dev); m.start()
        s = time.time()

        def session(path):
            for _ in range(per // b):
                c.exec_file(path)
        with ThreadPoolExecutor(max_workers=p) as ex:
            list(ex.map(session, files))
        e = time.time()
        print(f"sessions {p} x {per} rows: {_line(p * per, e - s, m.stop(), cpus)}", flush=True)
    for k in range(1, 9):
        c.trunc(f"t{k}")


def main(argv=None):
    a = sys.argv[1:] if argv is None else argv
    if len(a) < 2 or a[0] not in ("pg", "my") or a[1] not in ("size", "sessions"):
        print(USAGE, file=sys.stderr)
        return 2
    engine, phase = a[0], a[1]
    dev = os.environ.get("DEV", "vda")
    cpus = os.cpu_count() or 1
    c = Client(engine, os.environ.get("DIR", "."))
    with tempfile.TemporaryDirectory(prefix="calib-files-") as tmp:
        if phase == "size":
            sizes = [int(x) for x in os.environ.get("SIZES", "250000 500000 1000000 2000000 4000000").split()]
            size_phase(c, dev, cpus, int(os.environ.get("TOTAL", "4000000")), sizes, tmp)
        else:
            session_phase(c, dev, cpus, int(os.environ.get("B", "1000000")), int(os.environ.get("PER", "2000000")), tmp)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
