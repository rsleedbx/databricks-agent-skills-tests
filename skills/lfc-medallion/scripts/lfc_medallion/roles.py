"""roles: a first guess at each table's role (dimension, lookup, bridge, fact, log, queue) and its silver (scd1, scd2,
append), from a canonical schema (lfc-source-statschema's format, as a dict): keys, foreign keys, row counts, names, and
the activity (inserts, updates, deletes) when there is some. Each table gets its evidence (`why`), a confidence, and a
question to ask (`ask`) where a guess rests on little. The rules and their measured accuracy: cdc-table-plan's SKILL.md
and references/heuristics.md (that skill's classify.py is the command line over these functions).
"""
import re

LOG_NAME = re.compile(r"(^|_)(log|logs|history|audit|event|events|hits|stats|changes|messages?)($|_)|(Log|History|Message)$", re.I)
FACT_NAME = re.compile(r"(^|_)fact|^Fact", re.I)
QUEUE_NAME = re.compile(r"(^|_)queue($|_)|Queue$", re.I)
# a column that says rows are changed in place (ORM conventions: updated_at, date_modified, last_modified, modified_by)
UPDATED_COL = re.compile(r"^(date_)?(modified|updated|changed)(_at|_on|_date|_by|_dts)?$|^last_(modified|updated)|(^|_)updated_at$", re.I)
# a column that dates a row's creation (an event's time): date_added, created_at, date_submitted, date_hit, t_dts
CREATED_COL = re.compile(r"^(date_)?(added|created|submitted|hit|sent|triggered|occurred|logged)(_at|_on|_date)?$|^created|_dts$", re.I)
# a descriptive column: an entity or a definition (a campaign's step) has one, an event or a measurement rarely does
NAME_COL = re.compile(r"^(name|title|label|alias|description)$|_name$", re.I)
DIM_NAME = re.compile(r"(^|_)dim($|_)|^Dim", re.I)
WIDE = 8   # a link with this many columns of its own is a fact (TPC-C stock), not a membership (a segment's contacts)
NARROW = 4   # reference data has at most this many columns besides its keys (a code, a name, a description, a parent)
SILVER = {"dimension": ["scd1", "scd2"], "lookup": ["scd1"], "bridge": ["scd1"], "fact": ["scd1"], "queue": ["scd1"],
          "log": ["append"]}


def features(schema, sf, activity):
    tables = {t["name"]: t for t in schema["tables"]}
    lower = {n.lower(): n for n in tables}
    fks = {n: [] for n in tables}   # child -> [(columns, parent)]
    acting = {n: [] for n in tables}   # child -> its foreign keys whose ON DELETE changes it (CASCADE, SET NULL)
    for n, t in tables.items():
        seen = set()
        for fk in t.get("fk_constraints") or []:
            p = lower.get(str(fk["parent_table"]).lower())
            if p:
                fks[n].append((tuple(fk["columns"]), p)); seen.update(fk["columns"])
                if str(fk.get("on_delete") or "").upper().replace("_", " ") in ("CASCADE", "SET NULL", "SET DEFAULT"):
                    acting[n].append(f"{','.join(fk['columns'])} -> {p} ON DELETE {fk['on_delete']}")
        for c in t["columns"]:   # a column's own references: [parent, column]
            r = c.get("references")
            if r and c["name"] not in seen and lower.get(str(r[0]).lower()):
                fks[n].append(((c["name"],), lower[str(r[0]).lower()]))
    children = {n: set() for n in tables}
    for n, l in fks.items():
        for _, p in l:
            if p != n:
                children[p].add(n)
    pks = {n: {c["name"] for c in t["columns"] if c.get("primary_key")} for n, t in tables.items()}
    ident = {n: sorted({c for c, l in fks.items() for cols, p in l if p == n and c != n and set(cols) <= pks[c]
                        and set(cols) != pks[c]}) for n in tables}
    # a fixed size is known only in a scaled model (TPC: row_count_per_sf on the tables that grow); a collected
    # row_count is just the size today
    scaled = any(t.get("row_count_per_sf") for t in tables.values())
    out = {}
    for n, t in tables.items():
        pk = [c["name"] for c in t["columns"] if c.get("primary_key")]
        fixed = scaled and t.get("row_count") is not None and t.get("row_count_per_sf") is None
        # a scaled model: the growing tables' size per SF times --sf; a collected schema: its row_count as it is
        rows = t.get("row_count") if fixed or not scaled else (t.get("row_count_per_sf") or 0) * sf
        fkcols = {c for cols, _ in fks[n] for c in cols}
        parents = sorted({p for _, p in fks[n] if p != n})
        measures = [c["name"] for c in t["columns"] if c.get("type") in ("decimal", "double", "float", "real", "numeric")
                    and c["name"] not in fkcols and c["name"] not in pk]
        # one parent whose key is this table's key: a 1:1 extension of that parent
        ext = next((p for cols, p in fks[n] if p != n and set(cols) == set(pk)), None) if pk else None
        times = [c["name"] for c in t["columns"] if c.get("type") in ("timestamp", "datetime", "timestamptz")]
        out[n] = dict(pk=pk, fixed=fixed, rows=rows or 0, parents=parents, children=sorted(children[n]),
                      pk_is_fks=len(pk) >= 2 and set(pk) <= fkcols, ncols=len(t["columns"]),
                      # the columns of its own: not keys, not computed by the database (Mautic's generated_date_added_*)
                      other=len([c for c in t["columns"] if c["name"] not in fkcols and c["name"] not in pk and not c.get("generated")]),
                      created=[c["name"] for c in t["columns"] if CREATED_COL.search(c["name"]) and c.get("type") in ("timestamp", "date", "datetime")],
                      measures=measures, times=times, ext=ext,
                      updated=[c["name"] for c in t["columns"] if UPDATED_COL.search(c["name"])],
                      named_col=[c["name"] for c in t["columns"] if NAME_COL.search(c["name"])], identifies=ident[n], act=(activity or {}).get(n),
                      acting=acting[n])
    return out


def classify(n, f, feats, roles):
    why, ask = [], None
    np_, nc = len(f["parents"]), len(f["children"])
    why.append(f"{np_} parent(s){': ' + ', '.join(f['parents'][:4]) if np_ else ''}; "
               f"{nc} child(ren){': ' + ', '.join(f['children'][:4]) if nc else ''}")
    why.append(f"{'fixed ' if f['fixed'] else ''}{int(f['rows'])} rows; key {','.join(f['pk']) or 'none'}")
    a = f["act"]
    if a:
        why.append(f"activity: {a.get('inserts', 0)} inserts, {a.get('updates', 0)} updates, {a.get('deletes', 0)} deletes")
    prows = max([feats[p]["rows"] for p in f["parents"]] or [0])
    # 1. no key: nothing to merge on
    if not f["pk"]:
        return "log", ["append"], "high", why + ["no primary key: rows can only be appended"], None
    # 3. the name, when it says so (a weak hint: used only to break ties below)
    named = "fact" if FACT_NAME.search(n) else "dimension" if DIM_NAME.search(n) else "log" if LOG_NAME.search(n) else None
    if f["updated"]:
        why.append("changed in place (" + ", ".join(f["updated"][:2]) + ")")
    if QUEUE_NAME.search(n) and not nc:
        return "queue", ["scd1"], "medium", why + ["named like a queue"], \
            f"Are {n}'s rows inserted, then deleted when processed? Then scd1 (no history) is right."
    # 4. reference data: a fixed size, a few columns (a code, a name, a description), parents only reference data too
    if f["fixed"] and f["other"] <= NARROW and named not in ("fact", "dimension") and all(roles.get(p) == "lookup" for p in f["parents"]):
        return "lookup", ["scd1"], "high" if nc else "medium", why + [f"fixed size, {f['other']} column(s) besides its keys"], None
    # 5. a 1:1 extension of its only parent (the same key): the parent's role, once the parent has one
    if f["ext"] and np_ == 1 and roles.get(f["ext"]) in ("fact", "dimension", "log"):
        r = roles[f["ext"]]
        return r, SILVER[r], "medium", why + [f"a 1:1 extension of {f['ext']} (its key is a foreign key to it)"], \
            f"{n} extends {f['ext']} one to one: the same role ({r})?"
    # 6. a key made of foreign keys: a link between its parents; with columns of its own and nothing below it, a fact
    # (far larger than its largest parent, when sizes are known: TPC-DS inventory, a fact keyed by date, item, warehouse)
    if f["pk_is_fks"] and np_ >= 2 and (f["other"] <= 1 or nc or not f["measures"] and f["other"] < WIDE) and named not in ("fact", "log") \
            and not (prows and f["rows"] > 100 * prows):
        return "bridge", ["scd1"], "high" if f["other"] == 0 else "medium", why + ["its key is its foreign keys"], None
    # 7. a header: a fact child whose key holds its foreign key to this table (detail rows: order -> order lines), every
    # child a fact or a log, a parent of its own and at least as large as it
    if nc and np_ and named != "dimension" and f["rows"] >= prows / 2 and f["identifies"] \
            and all(roles.get(c) in ("fact", "log") or feats[c]["ext"] == n for c in f["children"]) \
            and any(roles.get(c) in ("fact", "log") for c in f["identifies"]):
        role = "fact"; why.append("the header of " + ", ".join(c for c in f["identifies"] if roles.get(c) in ("fact", "log")))
    # named like a log (history, audit, events, stats, hits) and no column saying it's changed in place: a log when
    # nothing references it, else a fact whose rows others point at (an email sent, then its opens)
    elif named == "log" and not f["updated"] and not (nc and f["named_col"]):
        role = "log" if nc == 0 else "fact"
    # a link with a key of its own (an ORM's surrogate id): two or more parents, nothing below, a few columns, no measures
    elif np_ == 2 and nc == 0 and len(f["pk"]) == 1 and not f["measures"] and f["other"] <= NARROW and not f["named_col"] \
            and not f["updated"] and named is None:
        return "bridge", ["scd1"], "medium", why + ["links its parents (a key of its own, a few columns besides)"], \
            f"Is {n} a membership (which {' and '.join(f['parents'][:2])} belong together)? Or events (then append)?"
    # 8. a fact: several parents, at least as large as the largest, nothing below it; or so named
    elif np_ >= 2 and f["rows"] >= prows and nc == 0 or named == "fact":
        role = "fact"
    elif np_ >= 1 and nc == 0 and f["rows"] > prows and (f["measures"] or named == "log") and named != "dimension":
        role = "log" if named == "log" else "fact"
    # an event row: a leaf under one parent, dated when added, never said to change, and no name (an opt-out, a note)
    elif np_ == 1 and nc == 0 and f["created"] and not f["updated"] and not f["named_col"] and named != "dimension" \
            and not f["ext"]:
        role = "fact"
        why.append(f"dated when added ({f['created'][0]}), no column saying it changes")
    else:
        role = "dimension" if nc or named == "dimension" or np_ <= 1 else "fact"
    if role == "dimension":
        conf = "high" if nc >= 2 else "medium"
        if not nc:
            ask = f"Nothing references {n}: is it a dimension (keep its history, scd2) or reference data (scd1 only)?"
        return "dimension", ["scd1", "scd2"], conf, why + (["referenced by other tables"] if nc else []), ask
    if role == "log":
        return "log", ["append"], "medium", why + ["named like a log, nothing references it"], \
            f"Is {n} insert-only? If its rows are ever updated or deleted, make it silver: [scd1]."
    conf = "high" if np_ >= 2 and nc == 0 else "medium"
    return "fact", ["scd1"], conf, why + ["the largest of its parents, few or no children"], \
        f"Are {n}'s rows ever updated or deleted? If it's insert-only, silver: [append] skips the MERGE."


def refine(n, f, role, silver, conf, why, ask):
    """The activity (inserts, updates, deletes since the statistics were reset) settles what the structure can't: whether
    a fact or a log is insert-only (append) or changed in place (scd1), and whether it's a queue. It doesn't make a
    dimension a log: a quiet dimension gets inserts only too, and a fixture load is inserts only."""
    a = f["act"]
    if not a or not a.get("inserts") and not a.get("updates") and not a.get("deletes"):
        return role, silver, conf, why, ask
    ins, upd, dele = a.get("inserts", 0), a.get("updates", 0), a.get("deletes", 0)
    if role in ("fact", "log", "queue") and ins and dele >= 0.5 * ins and upd <= ins and not f["children"]:
        return "queue", ["scd1"], "high", why + ["rows are inserted, then deleted"], None
    # inserts only, and nothing references it: appended. A fact others reference (an e-mail sent, later opened) can be
    # quiet for a while: a short window with no updates doesn't make it insert-only
    if role in ("fact", "log") and ins and not upd and not dele:
        if f["children"]:
            return role, silver, conf, why + ["inserts only so far, but other rows point at it: kept scd1"], \
                f"{n} had inserts only while the activity was counted; is it ever updated (append if never)?"
        return "log", ["append"], "high", why + ["inserts only: appended, no MERGE"], None
    if role == "log" and (upd or dele) and f["pk"]:
        return "fact", ["scd1"], "high", why + ["updated or deleted: not append-only"], None
    if role == "fact":
        return role, silver, "high", why + ["updated or deleted: scd1"], None
    return role, silver, conf, why, ask


def plan(schema, sf, activity, model):
    feats = features(schema, sf, activity)
    # in passes: a header is a fact when its children are, a lookup's parents are lookups; until nothing changes
    roles = {}
    for _ in range(len(feats) + 1):
        new = {n: classify(n, feats[n], feats, roles)[0] for n in feats}
        if new == roles:
            break
        roles = new
    out = {}
    for n in feats:
        role, silver, conf, why, ask = classify(n, feats[n], feats, roles)
        role, silver, conf, why, ask = refine(n, feats[n], role, silver, conf, why, ask)
        # not a guess: a parent's delete deletes (CASCADE) or changes (SET NULL) these rows, so they aren't insert-only,
        # whatever the role. The role stays; the rows need a MERGE (and on MySQL through 9.5 silver applies those
        # changes itself: the binlog doesn't carry them)
        if silver == ["append"] and feats[n]["pk"] and feats[n]["acting"]:
            silver, conf, ask = ["scd1"], "high", None
            acts = feats[n]["acting"]
            more = f" and {len(acts) - 1} more" if len(acts) > 1 else ""
            why = why + [f"its parent's deletes change its rows ({acts[0]}{more}): scd1, not append"]
        out[n] = {"role": role, "silver": list(silver), "confidence": conf, "decided_by": "relations", "why": "; ".join(why)}
        if ask and conf != "high":
            out[n]["ask"] = ask
    return {"version": 1, "model": model, "tables": out}
