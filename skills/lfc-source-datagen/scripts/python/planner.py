# The planner every engine shares: read the schema's tables from the catalog (engines/<engine>.py), pick each column's
# expression in x.n, and write one INSERT ... SELECT per table, which reads the table's current key range itself when it
# runs (so a rerun never duplicates rows). The same rules as the SQL planners (../postgres/plan-insert.sql and the
# others): a column's lfc:e={SQL} is its value as is; lfc:v={formula in x.n} replaces n in its cast; a one-column foreign
# key to the parent's one-column key takes a value inside the parent's current range; key, UNIQUE, foreign-key, and
# formula columns keep their own value, the rest are plain n, or scrambled per column (data=scramble | random) except
# in every plain_every-th block of `block` keys. A table qualifies with a one-column primary key that maps back to n,
# or with lfc:n={n from a row}; lfc:by={column} names the column n rises with; lfc:rows=N is its target; an append
# skips a table tagged lfc:fixed (every key value used); --grow SF appends to each table lfc:sf_rows x SF rows, so the
# tables keep their ratios and a fixed-size one (no lfc:sf_rows) gets none.
# The scramble's constants (a, b per column) come from a hash of the table and column (and --seed), so every plan of
# the same schema writes the same rows.
import re, zlib
from dataclasses import dataclass, field, replace

P = 2147483647                        # the scramble's modulus, 2^31 - 1


@dataclass
class Col:
    name: str
    pos: int
    ty: str                           # the engine's type name (engines/<engine>.py says which form)
    length: int | None = None         # characters or bytes as declared; -1: unbounded (text, CLOB, varchar(max))
    prec: int | None = None
    scale: int | None = None
    pk: bool = False
    unique: bool = False              # in a unique index or constraint: keeps plain n
    ident: bool = False               # an identity key column (the INSERT must override it)
    fk: str | None = None             # parent table, for a one-column foreign key to the parent's one-column primary key
    tags: str = ""                    # the column's comment and side-table tags
    x: dict = field(default_factory=dict)   # what else the engine's cast needs (type modifiers, enum values, ...)
    expr: str | None = None           # set by plan()


@dataclass
class Table:
    name: str
    cols: list
    pk: list                          # primary key column names, in key order
    tags: str = ""
    parents: set = field(default_factory=set)   # tables its foreign keys point at (any number of columns)
    level: int = 0
    pk_int: str | None = None         # n from a row: lfc:n, or the key decoded
    byc: str | None = None            # the (quoted) column n rises with
    has_n: bool = False
    target: int | None = None
    skipped: str | None = None        # why the planner leaves it out
    grow_rows: int | None = None      # an append by scale factor: this table's rows


@dataclass
class Opts:
    rows: int = 1000000               # at most this many rows per statement
    mode: str = "fill"                # fill: up to lfc:rows; append: rows more
    grow: float | None = None         # append by scale factor: each table lfc:sf_rows x grow rows (none without the tag)
    skip: frozenset = frozenset()     # tables to leave out (an append: the ones a signal rebuilds)
    order: str = "random"             # random | asc | desc
    data: str = "scramble"            # seq | scramble | random
    plain_every: int = 4
    block: int = 1000
    lob_len: int = 100
    seed: int = 0


def tag(s, k):
    m = re.search(r"lfc:%s=\{([^}]*)\}" % k, s or "")
    return m.group(1) if m else None


def rows_tag(s):
    m = re.search(r"lfc:rows=([0-9]+)", s or "")
    return int(m.group(1)) if m else None


def consts(o, *key):                  # a in 1 .. P - 2, b in 0 .. P - 1, fixed per table and column
    h = lambda s: zlib.crc32(f"{o.seed}:{'.'.join(key)}:{s}".encode())
    return 1 + h("a") % (P - 2), h("b") % P


def levels(tabs):                     # 0 for a table that references none in the schema, else 1 + its parents' highest
    by = {t.name: t for t in tabs}
    memo = {}
    def lv(n, depth=0):
        if n not in memo:
            ps = [p for p in by[n].parents if p in by and p != n]
            memo[n] = 0 if not ps or depth > 50 else 1 + max(lv(p, depth + 1) for p in ps)
        return memo[n]
    for t in tabs:
        t.level = lv(t.name)


def plan(eng, tabs, sch, o):
    """The INSERT for every table that qualifies: a list of (level, table, statement), by level and name."""
    levels(tabs)
    by = {t.name: t for t in tabs}
    for t in tabs:
        for c in t.cols:
            base = eng.cast(c, eng.length(c, o), o)
            e, v = tag(c.tags, "e"), tag(c.tags, "v")
            c.expr = e if e is not None else base.replace("x.n", f"({v})") if v is not None and base else base
            c.formula = e is not None or v is not None
        t.target = rows_tag(t.tags)
        nkey, cols = tag(t.tags, "n"), {c.name: c for c in t.cols}
        k1 = cols.get(t.pk[0]) if len(t.pk) == 1 else None
        t.has_n = nkey is not None
        t.pk_int = nkey if nkey is not None else eng.to_int(k1, eng.q(k1.name)) if k1 else None
        b = tag(t.tags, "by")
        t.byc = eng.qby(b) if b else eng.q(t.pk[0]) if t.pk else None
        if t.pk_int is None or t.byc is None:
            t.skipped = "no primary key" if not t.pk else "composite key" if len(t.pk) > 1 else f"key type {k1.ty if k1 else '?'}"
        elif o.mode == "fill" and t.target is None:
            t.skipped = "no lfc:rows target"
        elif o.mode == "append" and re.search(r"(^|\s)lfc:fixed(\s|$)", t.tags or ""):
            t.skipped = "fixed size: every key value used"
        elif t.name in o.skip:
            t.skipped = "left out (rebuilt by a signal)"
        elif o.mode == "append" and o.grow is not None:
            m = re.search(r"lfc:sf_rows=([0-9.]+)", t.tags or "")
            t.grow_rows = round(float(m.group(1)) * o.grow) if m else 0
            if not m:
                t.skipped = "fixed size: no lfc:sf_rows (it doesn't scale)"
            elif float(m.group(1)) * o.grow < 1:         # a parent that grows by no row breaks the children keyed under it
                raise ValueError(f"--grow {o.grow:g} gives {t.name} under one row ({m.group(1)} per scale factor): grow "
                                 f"by {1 / float(m.group(1)):g} or more, in whole steps of it")
    out = []
    for t in sorted(tabs, key=lambda t: (t.level, t.name)):
        if t.skipped:
            continue
        exprs = []
        for c in t.cols:
            x, p = c.expr, by.get(c.fk) if c.fk else None
            if p is not None and not c.pk and not c.formula and p.pk_int is not None and len(p.pk) == 1:
                # foreign key: inside the parent's current range of n, as the parent's key is made from n (with its
                # formula when the parent has lfc:n, since n is then the parent's n; else its plain cast of the key)
                pc = next(k for k in p.cols if k.name == p.pk[0])
                px = pc.expr if p.has_n else eng.cast(pc, eng.length(pc, o), o)
                if px is not None:
                    a, bb = consts(o, t.name, c.name, "fk")
                    lo, hi = eng.probe(sch, p, False), eng.probe(sch, p, True)
                    x = px.replace("x.n", eng.fk(lo, hi, eng.scr("x.r" if o.data == "random" else "x.n", a, bb)))
            elif not (o.data == "seq" or c.pk or c.unique or c.formula or c.fk) and x is not None:
                a, bb = consts(o, t.name, c.name)
                s = x.replace("x.n", eng.scr1("x.r" if o.data == "random" else "x.n", a, bb))
                x = eng.plain(o.block, o.plain_every, x, s) if o.plain_every > 0 else s
            exprs.append(x if x is not None else "NULL")
        ot = replace(o, rows=t.grow_rows) if o.mode == "append" and o.grow is not None else o   # its share of the growth
        out.append((t.level, t.name, eng.insert(sch, t, [c for c in t.cols], exprs, ot)))
    return out


def report(tabs):
    """What the plan leaves out: tables, and columns with no cast (they get NULL)."""
    lines = [f"table {t.name}: skipped ({t.skipped})" for t in tabs if t.skipped]
    lines += [f"column {t.name}.{c.name}: no cast for {c.ty}, NULL" for t in tabs if not t.skipped for c in t.cols if c.expr is None]
    return lines
