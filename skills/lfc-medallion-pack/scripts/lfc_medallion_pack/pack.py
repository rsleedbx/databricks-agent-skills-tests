"""pack: a model pack, everything that knows one model, in one folder: pack.yaml and the marts' SQL.

The runner (plan, source, verify) knows no model; a pack says, as data:

    name, model         the pack's name; the model its sources are built from (statschema's schemas/<model>.yaml, or a
                        schema.yaml in the pack: `schema: schema.yaml`)
    source_column       the column the n:1 layer adds to every table, naming the LOB (default lob)
    (tables and roles   read from the model's schema YAML, not listed here: each table and its role, fact or
                        dimension, as statschema's schema says)
    tiers               per tier (high, core, low: lobs.py), what a LOB's data looks like: its scale factor and its
                        signal numbers (lfc_source_datagen.signals: the model's signals: block, in order)
    marts               the SQL files of silver and gold, in order; they read all_<table> and key every join on the
                        source column
    readout             how the data test reads a signal back from gold: the signal, the gold table and its label, and
                        the rule's columns and formulas as gold names them (the rate comes from the signal's rule)
    contract            for an app and a model: grain, keys, label, features, measure

TPC packs ship in this package (packs/<name>); a customer's packs are folders in their own repo, passed by path.
"""
from __future__ import annotations

import os

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
BUILTIN = os.path.join(HERE, "packs")
REQUIRED = ("name", "model", "tiers", "marts")


def builtin() -> list[str]:
    """The packs this package ships."""
    return sorted(d for d in os.listdir(BUILTIN) if os.path.isfile(os.path.join(BUILTIN, d, "pack.yaml")))


def load(ref: str, base: str | None = None) -> dict:
    """A pack by name (a builtin one) or by path (a folder, relative to base); its folder is in `dir`."""
    path = ref if os.path.isabs(ref) else os.path.join(base or os.getcwd(), ref)
    if not os.path.isfile(os.path.join(path, "pack.yaml")):
        path = os.path.join(BUILTIN, ref)
    f = os.path.join(path, "pack.yaml")
    if not os.path.isfile(f):
        raise ValueError(f"pack {ref}: no pack.yaml there, and no builtin pack of that name ({', '.join(builtin())})")
    with open(f) as fh:
        p = yaml.safe_load(fh) or {}
    missing = [k for k in REQUIRED if k not in p]
    if missing:
        raise ValueError(f"pack {ref}: pack.yaml lacks {', '.join(missing)}")
    p.setdefault("source_column", "lob")
    p["dir"] = path
    if "tables" not in p:
        from lfc_source_datagen.sources import model_roles
        p["roles"] = model_roles(p["model"])
        p["tables"] = list(p["roles"])
    for t in ("high", "core", "low"):
        if t not in p["tiers"]:
            raise ValueError(f"pack {ref}: tiers lacks {t}")
    for m in p["marts"]:
        if not os.path.isfile(os.path.join(path, m)):
            raise ValueError(f"pack {ref}: marts file {m} isn't in {path}")
    return p


def mart_sql(p: dict) -> dict[str, str]:
    """{file name: SQL text} of the pack's marts, in order."""
    out = {}
    for m in p["marts"]:
        with open(os.path.join(p["dir"], m)) as fh:
            out[m] = fh.read()
    return out


def signals(p: dict, tier: str) -> str:
    """A tier's signal numbers, as lfc_source_datagen takes them (comma-separated; signals joined by /)."""
    s = p["tiers"][tier].get("signals", "")
    return ",".join(str(x) for x in s) if isinstance(s, list) else str(s)
