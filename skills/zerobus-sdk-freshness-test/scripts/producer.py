#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-zerobus-ingest-sdk>=1.10.0", "confluent-kafka>=2.5"]
# ///
"""producer.py --endpoint <host> --workspace-url <url> --table <catalog.schema.table> --client-id <id> (--secret-file F | --secret-env VAR) [options]:
write simulated tag observations to a Delta table through Zerobus Ingest (Python SDK, JSON records), and optionally the same events to a Kafka topic, at a steady rate with an optional burst.

  --endpoint        the Zerobus endpoint: <workspace-id>.zerobus.<region>.<cloud domain>
  --workspace-url   https://<workspace host>
  --table           an existing managed Delta table with the columns of table.sql
  --client-id       the service principal's application ID (granted on the table: zerobus-workspace-setup grant.sh)
  --secret-file F   a 0600 file holding its OAuth secret; or --secret-env VAR, an environment variable that holds it
                    (in a job: spark_env_vars VAR = {{secrets/<scope>/<key>}})
  --assets 200 --tags 15 --hz 1   events/s = assets x tags x hz (3,000 by default), plus a fault event per asset and tick while it has a fault
  --minutes 15      how long to send
  --streams 1       Zerobus streams, one thread each; the assets are split between them
  --burst-at M --burst-minutes 2 --burst-x 10
                    multiply the rate by --burst-x from minute M for --burst-minutes (no --burst-at: no burst)
  --dup-rate 0.001  share of events sent twice with the same event_id (at-least-once delivery upstream)
  --out run.json    sent counts, errors, and the run and burst windows (epoch seconds) for report.py

  Kafka fan-out (optional): the same events, at the same moments, to a Kafka topic as JSON keyed by asset_id
  --kafka-bootstrap host:port   e.g. <namespace>.servicebus.windows.net:9093 for Event Hubs; SASL_SSL, PLAIN
  --kafka-topic T               the topic (an event hub)
  --kafka-user U                the SASL user; for Event Hubs the literal $ConnectionString
  --kafka-password-file F       a 0600 file with the SASL password (Event Hubs: the connection string); or
                                --kafka-password-env VAR
  --no-zerobus                  Kafka only; the Zerobus options aren't needed then

Every event carries producer_time, the moment it was handed to the SDK, in epoch microseconds (Zerobus JSON takes
TIMESTAMP columns as microseconds). freshness_probe.py and report.py measure from that column.
"""
import argparse
import json
import os
import random
import sys
import threading
import time
import uuid
from pathlib import Path

from zerobus.sdk.shared import AckCallback, NonRetriableException, RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

SYSTEMS = ["ENGINE", "PUMP", "COOLING", "POWER", "MOTION", "ENV"]
MEASURES = [("TEMP", "degC", 0.0, 120.0), ("PRESSURE", "bar", 0.0, 16.0), ("SPEED", "rpm", 0.0, 3600.0),
            ("CURRENT", "A", 0.0, 400.0), ("VOLTAGE", "V", 360.0, 440.0), ("VIBRATION", "mm/s", 0.0, 25.0),
            ("FLOW", "m3/h", 0.0, 900.0), ("LEVEL", "pct", 0.0, 100.0), ("POSITION", "deg", 0.0, 360.0)]
STATES = ["IDLE", "RUNNING", "RUNNING", "RUNNING", "MAINTENANCE"]
FAULTS = ["OVER_TEMP", "LOW_PRESSURE", "HIGH_VIBRATION", "SENSOR_DISAGREE"]


def tag_defs(n):
    """n tags per asset: a state tag and n-1 numeric tags (system, measurement, unit, low, high)."""
    out = [("CONTROL", "STATE", None, None, None)]
    for i in range(n - 1):
        m, unit, lo, hi = MEASURES[i % len(MEASURES)]
        out.append((SYSTEMS[i % len(SYSTEMS)], f"{m}_{i // len(MEASURES) + 1}" if i >= len(MEASURES) else m, unit, lo, hi))
    return out


class Asset:
    def __init__(self, i, tags, rng):
        self.id, self.type, self.rng = f"ASSET-{i:05d}", rng.choice(["PUMP-A", "PUMP-B", "COMPRESSOR", "CONVEYOR"]), rng
        self.tags = tags
        self.vals = [None if t[3] is None else rng.uniform(t[3], t[4]) for t in tags]
        self.state, self.fault, self.fault_left = rng.choice(STATES), None, 0.0

    def readings(self, dt):
        r = self.rng
        if self.fault:
            self.fault_left -= dt
            self.fault = None if self.fault_left <= 0 else self.fault
        elif r.random() < dt / 6000:                     # about one fault episode per asset every 100 minutes
            self.fault, self.fault_left = r.choice(FAULTS), r.uniform(30, 120)
        if r.random() < dt / 600:
            self.state = r.choice(STATES)
        out = []
        for k, t in enumerate(self.tags):
            if t[3] is None:
                out.append((t, None, self.state))
                continue
            lo, hi = t[3], t[4]
            v = min(hi, max(lo, self.vals[k] + r.gauss(0, (hi - lo) * 0.01)))
            self.vals[k] = v
            if self.fault and k == 1:                    # a fault pushes the first numeric tag past its limit
                v = hi * 1.1
            out.append((t, round(v, 3), None))
        if self.fault:
            out.append((("CONTROL", "FAULT", None, None, None), None, self.fault))
        return out


class Stats(AckCallback):
    def __init__(self):
        super().__init__()
        self.lock, self.sent, self.acks, self.errors, self.reconnects, self.last_send = threading.Lock(), 0, 0, 0, 0, None
        self.kafka_sent, self.kafka_errors = 0, 0

    def on_ack(self, offset):
        with self.lock:
            self.acks += 1

    def on_error(self, offset, message):
        with self.lock:
            self.errors += 1
        print(f"zerobus error at offset {offset}: {message}", file=sys.stderr, flush=True)


def quality(r):
    x = r.random()
    return "BAD" if x < 0.0005 else "UNCERTAIN" if x < 0.002 else "GOOD"


def main():
    p = argparse.ArgumentParser(usage=__doc__.split("\n")[0])
    for a in ("--endpoint", "--workspace-url", "--table", "--client-id"):
        p.add_argument(a)
    p.add_argument("--no-zerobus", action="store_true")
    p.add_argument("--kafka-bootstrap")
    p.add_argument("--kafka-topic")
    p.add_argument("--kafka-user")
    p.add_argument("--kafka-password-file")
    p.add_argument("--kafka-password-env")
    p.add_argument("--secret-file")
    p.add_argument("--secret-env")
    p.add_argument("--assets", type=int, default=200)
    p.add_argument("--tags", type=int, default=15)
    p.add_argument("--hz", type=float, default=1.0)
    p.add_argument("--minutes", type=float, default=15.0)
    p.add_argument("--streams", type=int, default=1)
    p.add_argument("--burst-at", type=float)
    p.add_argument("--burst-minutes", type=float, default=2.0)
    p.add_argument("--burst-x", type=float, default=10.0)
    p.add_argument("--dup-rate", type=float, default=0.001)
    p.add_argument("--producer-id", default="producer")
    p.add_argument("--out", default="run.json")
    if len(sys.argv) == 1:
        print(__doc__)
        return
    a = p.parse_args()
    use_zb, use_kafka = not a.no_zerobus, bool(a.kafka_bootstrap)
    if not (use_zb or use_kafka):
        sys.exit("--no-zerobus needs --kafka-bootstrap")
    if use_zb:
        missing = [o for o in ("endpoint", "workspace_url", "table", "client_id") if not getattr(a, o)]
        if missing:
            sys.exit("missing " + ", ".join("--" + m.replace("_", "-") for m in missing))
        secret = (Path(a.secret_file).read_text().strip() if a.secret_file
                  else os.environ.get(a.secret_env, "") if a.secret_env else sys.exit("--secret-file or --secret-env is required"))
        if not secret:
            sys.exit("the OAuth secret is empty")
    if use_kafka:
        if not (a.kafka_topic and a.kafka_user):
            sys.exit("--kafka-bootstrap needs --kafka-topic and --kafka-user")
        kpw = (Path(a.kafka_password_file).read_text().strip() if a.kafka_password_file
               else os.environ.get(a.kafka_password_env, "") if a.kafka_password_env else "")
        if not kpw:
            sys.exit("--kafka-password-file or --kafka-password-env is required, and not empty")

    stats, stop = Stats(), threading.Event()
    if use_zb:
        sdk = ZerobusSdk(a.endpoint, a.workspace_url)
        opts = StreamConfigurationOptions(record_type=RecordType.JSON, ack_callback=stats)
        open_stream = lambda: sdk.create_stream(a.client_id, secret, TableProperties(a.table), opts)

    def open_kafka():
        from confluent_kafka import Producer                          # only needed with --kafka-bootstrap
        return Producer({"bootstrap.servers": a.kafka_bootstrap, "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
                         "sasl.username": a.kafka_user, "sasl.password": kpw, "linger.ms": 5, "batch.size": 262144,
                         "acks": "1", "client.id": a.producer_id})

    def on_delivery(err, _msg):
        if err:
            with stats.lock:
                stats.kafka_errors += 1
    tags = tag_defs(a.tags)
    rng = random.Random(42)
    assets = [Asset(i, tags, random.Random(rng.random())) for i in range(a.assets)]
    t0 = time.time()
    burst = (t0 + a.burst_at * 60, t0 + (a.burst_at + a.burst_minutes) * 60) if a.burst_at is not None else None
    deadline = t0 + a.minutes * 60

    def rate_x(now):
        return a.burst_x if burst and burst[0] <= now < burst[1] else 1.0

    def worker(idx, group):
        stream = open_stream() if use_zb else None
        kafka = open_kafka() if use_kafka else None
        r, slices = random.Random(idx), [group[i::10] for i in range(min(10, len(group)))]
        next_t = time.monotonic()
        try:
            while not stop.is_set() and time.time() < deadline:
                period = 1.0 / (a.hz * rate_x(time.time()))
                for sl in slices:
                    now_us = int(time.time() * 1_000_000)
                    batch = []
                    for asset in sl:
                        obs_us = now_us - r.randint(5_000, 150_000)          # sensor -> producer delay
                        for (system, meas, unit, lo, hi), v, vt in asset.readings(period):
                            batch.append({"event_id": str(uuid.uuid4()), "observation_time": obs_us, "producer_time": now_us,
                                          "tag_id": f"{asset.id}.{system}.{meas}", "value": v, "value_text": vt,
                                          "quality": quality(r), "asset_id": asset.id, "asset_type": asset.type,
                                          "system": system, "measurement": meas, "unit": unit, "eng_low": lo,
                                          "eng_high": hi, "producer_id": f"{a.producer_id}-{idx}"})
                    batch.extend([e for e in batch if r.random() < a.dup_rate])
                    if stream is not None:
                        try:
                            stream.ingest_records_nowait(batch)
                        except NonRetriableException:
                            raise
                        except Exception as e:                                # the SDK has retried; reopen, replay unacked
                            print(f"stream {idx} failed ({e}); reopening", file=sys.stderr, flush=True)
                            pending = list(stream.get_unacked_records())
                            try:
                                stream.close()
                            except Exception:
                                pass
                            stream = open_stream()
                            with stats.lock:
                                stats.reconnects += 1
                            stream.ingest_records_nowait(pending + batch)
                    if kafka is not None:
                        for e in batch:
                            while True:
                                try:
                                    kafka.produce(a.kafka_topic, key=e["asset_id"], value=json.dumps(e), on_delivery=on_delivery)
                                    break
                                except BufferError:                           # the local queue is full: let it drain
                                    kafka.poll(0.05)
                        kafka.poll(0)
                        with stats.lock:
                            stats.kafka_sent += len(batch)
                    with stats.lock:
                        stats.sent += len(batch)
                        stats.last_send = time.time()
                    next_t += period / len(slices)
                    delay = next_t - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    elif delay < -5 * period:
                        next_t = time.monotonic()                             # far behind: don't try to catch up
        finally:
            if kafka is not None:
                left = kafka.flush(60)                                        # still queued: never delivered
                if left:
                    print(f"stream {idx}: {left} Kafka messages not delivered", file=sys.stderr, flush=True)
                    with stats.lock:
                        stats.kafka_errors += left
            if stream is not None:
                stream.flush()
                stream.close()

    groups = [assets[i::a.streams] for i in range(a.streams)]
    threads = [threading.Thread(target=worker, args=(i, g), daemon=True) for i, g in enumerate(groups) if g]
    print(f"{len(threads)} stream(s), ~{a.assets * a.tags * a.hz:,.0f} events/s for {a.minutes:g} min"
          + (f", x{a.burst_x:g} from minute {a.burst_at:g} for {a.burst_minutes:g} min" if burst else ""), flush=True)
    for t in threads:
        t.start()
    last, last_t = 0, time.time()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(10)
            with stats.lock:
                s, e, rc, ke = stats.sent, stats.errors, stats.reconnects, stats.kafka_errors
            now = time.time()
            print(f"[{now - t0:5.0f}s] sent={s:,} rate={(s - last) / (now - last_t):,.0f}/s errors={e} reconnects={rc}"
                  + (f" kafka_errors={ke}" if use_kafka else ""), flush=True)
            last, last_t = s, now
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=120)
    t1 = stats.last_send or time.time()                                    # the last send, not when the threads exited
    out = {"table": a.table if use_zb else None, "kafka_topic": a.kafka_topic if use_kafka else None,
           "start": t0, "end": t1, "burst": burst, "sent": stats.sent, "errors": stats.errors,
           "kafka_sent": stats.kafka_sent if use_kafka else None, "kafka_errors": stats.kafka_errors if use_kafka else None,
           "reconnects": stats.reconnects, "streams": a.streams, "assets": a.assets, "tags": a.tags, "hz": a.hz,
           "burst_x": a.burst_x if burst else None, "dup_rate": a.dup_rate}
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(out, indent=1))
    print(f"done: sent={stats.sent:,} in {t1 - t0:,.0f} s, errors={stats.errors}, reconnects={stats.reconnects}"
          + (f", kafka_errors={stats.kafka_errors}" if use_kafka else "") + f"; wrote {a.out}")
    if stats.errors or stats.kafka_errors:                                   # no sys.exit(0): a Databricks Python task counts any SystemExit as a failure
        sys.exit(1)


if __name__ == "__main__":
    main()
