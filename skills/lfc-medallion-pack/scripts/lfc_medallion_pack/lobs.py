"""lobs: the lines of business (LOBs, or tenants, divisions, acquired companies) of one module, and their tiers.

Most companies have many LOBs, and how they perform follows the 20-70-10 vitality curve: each LOB gets a score drawn
from a normal distribution (seeded, so every rerun gives the same LOBs), then by rank the top 20% are `high`, the
middle 70% `core` and the bottom 10% `low`. Nothing here knows a model: a pack says what each tier's data looks like
(its signal numbers), so the curve reaches the data through the pack.

    lobs(250, seed=7)                    # [{"name": "lob_001", "score": 1.62, "tier": "high"}, ...]
    lobs(names=["east", "west", "north"])
"""
from __future__ import annotations

import random
import re

TIERS = ("high", "core", "low")
NAME = re.compile(r"^[a-z][a-z0-9_]{0,30}$")


def cuts(n: int, high: float = 0.2, low: float = 0.1) -> tuple[int, int]:
    """(how many high, how many low) of n LOBs: the shares rounded, at least one low and one high once n allows."""
    h, lo = round(n * high), round(n * low)
    if n >= 3:
        h, lo = max(h, 1), max(lo, 1)
    return h, lo


def lobs(n: int | None = None, seed: int = 1, names: list[str] | None = None, high: float = 0.2,
         low: float = 0.1) -> list[dict]:
    """n LOBs named lob_001.. (or the names given), each with a score (normal, seeded) and a tier by rank; in name
    order."""
    names = list(names) if names else [f"lob_{k:03d}" for k in range(1, (n or 0) + 1)]
    if not names:
        raise ValueError("lobs: n or names")
    bad = [x for x in names if not NAME.match(x)]
    if bad or len(set(names)) != len(names):
        raise ValueError(f"lobs: names must be unique, lower-case letters, digits and _ (at most 31): {bad or names}")
    rnd = random.Random(seed)
    score = {x: round(rnd.gauss(0, 1), 4) for x in names}
    ranked = sorted(names, key=lambda x: (-score[x], x))
    h, lo = cuts(len(names), high, low)
    tier = {x: "high" if k < h else "low" if k >= len(names) - lo else "core" for k, x in enumerate(ranked)}
    return [{"name": x, "score": score[x], "tier": tier[x]} for x in names]
