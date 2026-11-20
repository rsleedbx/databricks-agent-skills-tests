#!/usr/bin/env python3
"""validate-model.py <model.yaml>...: check statschema model files against model format 1 (lfc-workload-contract).

Prints one line per problem, "<file>: <problem>" (a parse error: "<file>:<line>: <problem>"), and exits 1 if there is any.
Prints nothing and exits 0 when every file holds. Exits 2 with the usage when it is called with no file. Stdlib only.

Model format 1 (what a consumer may rely on): a mapping with
  version              the string "1.0"
  tables               a non-empty list; each table a mapping with
    name               a non-empty string, unique in the file
    row_count          a whole number, 0 or more   (fixed size), or
    row_count_per_sf   a number above 0            (rows per scale factor unit): exactly one of the two
    columns            a non-empty list; each column a mapping with
      name             a non-empty string, unique in its table
      type             a non-empty string (statschema's canonical type)
      primary_key      true or false (optional); the table's key is its columns with primary_key: true, none for a keyless table
    formulas           a mapping of names to strings (optional): sub-formulas for the columns' generation rules
Other keys are ignored: a new optional key keeps the format at 1.

The reader handles the YAML subset the model files use: block mappings and sequences at any indentation (tables at 0 or 2
spaces), flow mappings and sequences on one line, quoted and plain scalars, comments. Block scalars (| and >), anchors, tags,
tabs and multi-line plain scalars are refused with the line number.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

KEY = re.compile(r"^([A-Za-z_][A-Za-z0-9_.\-]*)\s*:(?:\s+(.*))?$")
NUM_FLOAT = re.compile(r"^-?(\d+\.\d*|\.\d+|\d+(\.\d*)?[eE][-+]?\d+)$")


class Bad(Exception):
    def __init__(self, line: int, msg: str):
        super().__init__(msg)
        self.line, self.msg = line, msg


# --- the reader ------------------------------------------------------------------------------------------------------

def strip_comment(s: str) -> str:
    """Drop a comment: a '#' at the start or after a space, outside quotes. A quote opens a scalar only where one can start."""
    out, q, prev = [], None, ""
    for i, ch in enumerate(s):
        if q:
            out.append(ch)
            if ch == q:
                q, prev = None, ch
            continue
        if ch == "#" and (i == 0 or s[i - 1] in " \t"):
            break
        if ch in "\"'" and prev in ("", ":", "[", "{", ",", "-"):
            q = ch
        out.append(ch)
        if not ch.isspace():
            prev = ch
    return "".join(out)


def scalar(text: str):
    if text in ("true", "false"):
        return text == "true"
    if text in ("null", "~", ""):
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if NUM_FLOAT.match(text):
        return float(text)
    return text


class Flow:
    """A flow collection or quoted string on one line: {a: 1, b: [x, y]}, ['x', "y"]."""

    def __init__(self, text: str, line: int):
        self.s, self.i, self.line = text, 0, line

    def peek(self) -> str:
        return self.s[self.i] if self.i < len(self.s) else ""

    def ws(self) -> None:
        while self.i < len(self.s) and self.s[self.i] == " ":
            self.i += 1

    def value(self):
        self.ws()
        c = self.peek()
        if c == "":
            raise Bad(self.line, "a flow collection ends before its value")
        if c == "{":
            return self.collection("}", True)
        if c == "[":
            return self.collection("]", False)
        if c in "\"'":
            return self.quoted()
        return self.plain(",]}")

    def collection(self, close: str, mapping: bool):
        self.i += 1
        out = {} if mapping else []
        self.ws()
        if self.peek() == close:
            self.i += 1
            return out
        while True:
            self.ws()
            if mapping:
                key = self.quoted() if self.peek() in "\"'" else self.plain(":,}")
                if not isinstance(key, str) or not key:
                    raise Bad(self.line, "a flow mapping key is missing")
                self.ws()
                if self.peek() != ":":
                    raise Bad(self.line, f"flow mapping: {key} has no value")
                self.i += 1
                if key in out:
                    raise Bad(self.line, f"duplicate key {key}")
                out[key] = self.value()
            else:
                out.append(self.value())
            self.ws()
            c = self.peek()
            if c == ",":
                self.i += 1
            elif c == close:
                self.i += 1
                return out
            else:
                raise Bad(self.line, f"expected ',' or '{close}' in a flow collection")

    def quoted(self):
        q = self.s[self.i]
        j, buf = self.i + 1, []
        while j < len(self.s):
            ch = self.s[j]
            if q == '"' and ch == "\\" and j + 1 < len(self.s):
                buf.append(self.s[j + 1])
                j += 2
                continue
            if ch == q:
                if q == "'" and j + 1 < len(self.s) and self.s[j + 1] == "'":
                    buf.append("'")
                    j += 2
                    continue
                self.i = j + 1
                return "".join(buf)
            buf.append(ch)
            j += 1
        raise Bad(self.line, "a quoted string is not closed on its line")

    def plain(self, stop: str):
        j = self.i
        while j < len(self.s) and self.s[j] not in stop:
            j += 1
        text = self.s[self.i:j].strip()
        self.i = j
        return scalar(text)


def inline(text: str, line: int):
    """A value written after 'key:' or '- ' on its own line."""
    if text[:1] in "{[":
        f = Flow(text, line)
        v = f.value()
        f.ws()
        if f.i != len(text):
            raise Bad(line, "text after a flow collection")
        return v
    if text[:1] in "\"'":
        f = Flow(text, line)
        v = f.quoted()
        f.ws()
        if f.i != len(text):
            raise Bad(line, "text after a quoted string")
        return v
    if text[:1] in "|>&*!%@`":
        raise Bad(line, "block scalars, anchors and tags are not in model format 1: use a one-line quoted string")
    return scalar(text)


class Block:
    """Indentation-based block reader over [(line number, indent, text)]."""

    def __init__(self, lines):
        self.L, self.i = lines, 0

    def at(self, k):
        return self.L[k] if k < len(self.L) else None

    def node(self, ind: int):
        c = self.L[self.i][2]
        if c == "-" or c.startswith("- "):
            return self.seq(ind)
        return self.map(ind)

    def seq(self, ind: int):
        out = []
        while self.i < len(self.L):
            n, k, c = self.L[self.i]
            if k != ind or not (c == "-" or c.startswith("- ")):
                break
            rest = c[1:].lstrip()
            if rest == "":
                self.i += 1
                nxt = self.at(self.i)
                out.append(self.node(nxt[1]) if nxt and nxt[1] > ind else None)
            elif KEY.match(rest) and rest[:1] not in "{[\"'":
                off = len(c) - len(rest)            # the item is a mapping: its first key sits on the dash line
                self.L[self.i] = [n, ind + off, rest]
                out.append(self.node(ind + off))
            else:
                self.i += 1
                out.append(inline(self.folded(rest, ind), n))
        return out

    def map(self, ind: int):
        out = {}
        while self.i < len(self.L):
            n, k, c = self.L[self.i]
            if k < ind:
                break
            if k > ind:
                raise Bad(n, "unexpected indentation (a multi-line value is not in model format 1)")
            if c == "-" or c.startswith("- "):
                break
            m = KEY.match(c)
            if not m:
                raise Bad(n, f"not a 'key: value' line: {c[:60]}")
            key, val = m.group(1), m.group(2)
            if key in out:
                raise Bad(n, f"duplicate key {key}")
            self.i += 1
            nxt = self.at(self.i)
            if val is None or val == "":
                if nxt and nxt[1] > ind:
                    out[key] = self.node(nxt[1])
                elif nxt and nxt[1] == ind and (nxt[2] == "-" or nxt[2].startswith("- ")):
                    out[key] = self.seq(ind)
                else:
                    out[key] = None
            else:
                out[key] = inline(self.folded(val, ind), n)
        return out

    def folded(self, val: str, ind: int) -> str:
        """A scalar that a YAML writer wrapped (yaml.safe_dump folds long values): the deeper-indented lines after a
        non-empty value can only continue it; join them with one space, as YAML folds them."""
        while self.i < len(self.L) and self.L[self.i][1] > ind:
            val += " " + self.L[self.i][2]
            self.i += 1
        return val


def bracket_delta(s: str) -> int:
    """Open minus close brackets outside quotes: a flow collection that runs on to the next line has a positive count."""
    depth, q, prev = 0, None, ""
    for ch in s:
        if q:
            if ch == q:
                q, prev = None, ch
            continue
        if ch in "\"'" and prev in ("", ":", "[", "{", ",", "-"):
            q = ch
            continue
        if ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        if not ch.isspace():
            prev = ch
    return depth


def parse(text: str):
    lines, pending = [], None   # pending: [line, indent, text, open brackets] while a flow collection runs on
    for n, raw in enumerate(text.splitlines(), 1):
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise Bad(n, "a tab in the indentation")
        s = strip_comment(raw).rstrip()
        if pending is not None:
            pending[2] += " " + s.strip()
            pending[3] += bracket_delta(s)
            if pending[3] <= 0:
                lines.append(pending[:3])
                pending = None
            continue
        if not s.strip():
            continue
        text_ = s.strip()
        depth = bracket_delta(text_)
        if depth > 0:
            pending = [n, len(s) - len(s.lstrip(" ")), text_, depth]
        else:
            lines.append([n, len(s) - len(s.lstrip(" ")), text_])
    if pending is not None:
        raise Bad(pending[0], "a flow collection is not closed")
    if not lines:
        return None
    reader = Block(lines)
    value = reader.node(lines[0][1])
    if reader.i != len(lines):
        n, _, c = lines[reader.i]
        raise Bad(n, f"unexpected content: {c[:60]}")
    return value


# --- the format ------------------------------------------------------------------------------------------------------

def is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def problems_of(data) -> list[str]:
    if not isinstance(data, dict):
        return ["the top level is not a mapping (version and tables)"]
    out = []
    if "version" not in data:
        out.append('missing version (write version: "1.0")')
    elif data["version"] != "1.0" or not isinstance(data["version"], str):
        out.append(f'version is {data["version"]!r}: model format 1 needs the string "1.0" (quoted)')
    tables = data.get("tables")
    if "tables" not in data:
        return out + ["missing tables"]
    if not isinstance(tables, list) or not tables:
        return out + ["tables must be a non-empty list"]
    seen = set()
    for k, t in enumerate(tables, 1):
        if not isinstance(t, dict):
            out.append(f"table #{k} is not a mapping")
            continue
        name = t.get("name")
        if not isinstance(name, str) or not name:
            out.append(f"table #{k} has no name")
            label = f"table #{k}"
        else:
            label = f"table {name}"
            if name in seen:
                out.append(f"{label}: duplicate table name")
            seen.add(name)
        have = [key for key in ("row_count", "row_count_per_sf") if key in t]
        if len(have) != 1:
            out.append(f"{label}: needs exactly one of row_count (a whole number, 0 or more) or row_count_per_sf (a number above 0)")
        elif have[0] == "row_count" and not (is_int(t["row_count"]) and t["row_count"] >= 0):
            out.append(f"{label}: row_count must be a whole number, 0 or more")
        elif have[0] == "row_count_per_sf" and not (is_num(t["row_count_per_sf"]) and t["row_count_per_sf"] > 0):
            out.append(f"{label}: row_count_per_sf must be a number above 0")
        if "formulas" in t and not (isinstance(t["formulas"], dict) and all(isinstance(v, str) for v in t["formulas"].values())):
            out.append(f"{label}: formulas must be a mapping of names to strings")
        cols = t.get("columns")
        if not isinstance(cols, list) or not cols:
            out.append(f"{label}: columns must be a non-empty list")
            continue
        cnames = set()
        for c in cols:
            if not isinstance(c, dict):
                out.append(f"{label}: a column is not a mapping")
                continue
            cn = c.get("name")
            if not isinstance(cn, str) or not cn:
                out.append(f"{label}: a column has no name")
                continue
            if cn in cnames:
                out.append(f"{label}: column {cn} is listed twice")
            cnames.add(cn)
            if not isinstance(c.get("type"), str) or not c.get("type"):
                out.append(f"{label}: column {cn} has no type")
            if "primary_key" in c and not isinstance(c["primary_key"], bool):
                out.append(f"{label}: column {cn}: primary_key must be true or false")
    return out


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: validate-model.py <model.yaml>...", file=sys.stderr)
        return 2
    bad = 0
    for f in argv:
        try:
            text = Path(f).read_text(encoding="utf-8")
        except OSError as e:
            print(f"{f}: {e.strerror or e}")
            bad += 1
            continue
        try:
            data = parse(text)
        except Bad as e:
            print(f"{f}:{e.line}: {e.msg}")
            bad += 1
            continue
        for p in problems_of(data):
            print(f"{f}: {p}")
            bad += 1
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
