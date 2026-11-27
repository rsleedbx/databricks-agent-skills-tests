#!/usr/bin/env python3
"""keep_catalogs.py <profile> [--connection N]... [catalog]...: set the RemoveAfter tag on test catalogs and connections
to a month out.

Some workspaces tag a catalog that has no RemoveAfter tag with a date a couple of weeks out and delete it on that date.
This sets RemoveAfter to today + 1 month (KEEP_UNTIL=YYYY-MM-DD for another date; KEEP_FROM=YYYY-MM-DD counts the month
from that day; a month end stays in range: Jan 31 -> Feb 28) on each catalog named, and with --connection N on
connection N and every foreign catalog made from it. It never moves a later date earlier. One line per catalog (a
connection's line says connection): kept (already later), set or updated, or failed with the API's error; "none" for a
--connection with no foreign catalog. Exit 1 if any failed (a failed catalogs list too), 2 on a bad date.

Needs APPLY TAG on the catalog or connection (or ownership); a governed RemoveAfter tag also needs ASSIGN on its policy.
REST through lfc-rest's Rest, so it runs from a laptop and in a notebook (`from keep_catalogs import keep`).
"""
from __future__ import annotations

import calendar
import datetime as dt
import os
import re
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest, not_found

TAGS = "/api/2.1/unity-catalog/entity-tag-assignments"
KEY = "RemoveAfter"


def month_out(start: dt.date) -> str:
    """A month after start, the day kept in range (Jan 31 -> Feb 28 or 29)."""
    y, m = (start.year + 1, 1) if start.month == 12 else (start.year, start.month + 1)
    return dt.date(y, m, min(start.day, calendar.monthrange(y, m)[1])).isoformat()


def until_date(env=os.environ, today=None) -> str:
    """KEEP_UNTIL, or a month out from KEEP_FROM (or today); ValueError when not YYYY-MM-DD."""
    if env.get("KEEP_UNTIL"):
        u = env["KEEP_UNTIL"]
    else:
        f = env.get("KEEP_FROM")
        u = month_out(dt.date.fromisoformat(f) if f else (today or dt.datetime.now(dt.timezone.utc).date())) if not f or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", f) else ""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", u or ""):
        raise ValueError(f"no RemoveAfter date (KEEP_UNTIL / KEEP_FROM are YYYY-MM-DD): {u!r}")
    return u


def keep(api, catalogs=(), connections=(), until: str = "", out=None) -> int:
    """The tags set (module doc); the number that failed."""
    api, out, bad = Rest.of(api), out or sys.stdout, 0
    todo = [("catalogs", c) for c in catalogs]
    if connections:
        try:
            listed = list(api.pages("/api/2.1/unity-catalog/catalogs", "catalogs"))
        except Exception as e:  # noqa: BLE001  the named catalogs are still tagged
            print(f"failed  catalogs list: {str(e)[:200]}", file=out)
            bad, listed = bad + 1, None
        for c in connections:
            todo.append(("connections", c))
            if listed is None:
                continue
            found = [x["name"] for x in listed if x.get("catalog_type") == "FOREIGN_CATALOG" and x.get("connection_name") == c]
            if not found:
                print(f"none    foreign catalogs of {c}", file=out)
            todo += [("catalogs", n) for n in found]
    for ty, name in todo:
        label = f"connection {name}" if ty == "connections" else name
        path = f"{TAGS}/{ty}/{quote(name, safe='')}/tags/{KEY}"
        try:
            cur = api.get(path).get("tag_value") or ""
        except Exception as e:  # noqa: BLE001
            if not not_found(e):
                print(f"failed  {label}: {str(e)[:200]}", file=out)
                bad += 1
                continue
            cur = ""
        if cur and cur >= until:
            print(f"kept    {label} {KEY}={cur}", file=out)
            continue
        body = {"entity_type": ty, "entity_name": name, "tag_key": KEY, "tag_value": until}
        try:
            if cur:
                api.patch(path, body, update_mask="tag_value")
            else:
                api.post(TAGS, body)
        except Exception as e:  # noqa: BLE001  the API's error, and the others still run
            print(f"failed  {label}: {str(e)[:200]}", file=out)
            bad += 1
            continue
        print(f"{'updated' if cur else 'set':<7} {label} {KEY}={until}{f' (was {cur})' if cur else ''}", file=out)
    return bad


def main(argv=None, api=None, env=os.environ) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0].startswith("-"):
        print("usage: keep_catalogs.py <profile> [--connection N]... [catalog]...")
        return 2
    prof, cats, conns, rest = argv[0], [], [], argv[1:]
    while rest:
        x = rest.pop(0)
        if x == "--connection":
            if not rest:
                print("--connection needs a name")
                return 2
            conns.append(rest.pop(0))
        elif x.startswith("-"):
            print(f"unknown {x}")
            return 2
        else:
            cats.append(x)
    try:
        until = until_date(env)
    except ValueError as e:
        print(e)
        return 2
    if not cats and not conns:
        return 0
    return 1 if keep(api or Rest.connect(prof), cats, conns, until) else 0


if __name__ == "__main__":
    sys.exit(main())
