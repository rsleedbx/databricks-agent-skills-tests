#!/usr/bin/env python3
"""tpc_schema.py <tpcb|tpch> [SF]: TPC-B and TPC-H as MySQL DDL with the lfc tags, written from the spec (keys, foreign
keys, checks; the population formulas as lfc:v / lfc:n tags). The same text as tpc/tpc-schema.sh, which runs this for
tpcb and tpch; statschema's ddl_to_schema() and ddl() turn it into any engine's DDL.
From Python: from lfc_source_datagen.tpc_schema import tpc_schema; tpc_schema("tpch", 0.1).
TPC-B: per branch, 10 tellers and 100,000 accounts (clause 4.2), records padded to the minimum lengths (clause 3.2);
history rows default to the accounts' (HISTORY on the command line, history= here).
TPC-H: clause 1.4 layouts, 1.4.2 constraints, 4.2.3 population; LINEITEM has 4 lines per order (the spec: 1 to 7,
random); scale factor 0.03 or more (partsupp's key is read back from the spec's supplier formula, which needs
3 * (S/4 + 19) < S: at least 230 suppliers).
"""
from __future__ import annotations

import os
import sys
from string import Template


def h(e: str, a: int) -> str:
    """A non-linear scramble of integer expression e (constant a), 0 .. 2^31 - 2, the same on every engine."""
    return f"((({e}) % 2147483647) * ((({e}) % 2147483647 + {a}) % 2147483647) % 2147483647)"


def _int(x: float) -> int:
    return int(x)                                   # as awk's printf "%d": toward zero


TPCB = Template("CREATE TABLE branch (b_id int NOT NULL, b_balance decimal(12,2) NOT NULL, b_filler char(88),\n  PRIMARY KEY (b_id)) COMMENT='lfc:rows=${B}';\nCREATE TABLE teller (t_id int NOT NULL, t_b_id int NOT NULL COMMENT 'lfc:v={(x.n - 1) DIV 10 + 1}',\n  t_balance decimal(12,2) NOT NULL, t_filler char(84),\n  PRIMARY KEY (t_id), FOREIGN KEY (t_b_id) REFERENCES branch (b_id)) COMMENT='lfc:rows=${T}';\nCREATE TABLE account (a_id int NOT NULL, a_b_id int NOT NULL COMMENT 'lfc:v={(x.n - 1) DIV 100000 + 1}',\n  a_balance decimal(12,2) NOT NULL, a_filler char(84),\n  PRIMARY KEY (a_id), FOREIGN KEY (a_b_id) REFERENCES branch (b_id)) COMMENT='lfc:rows=${A}';\nCREATE TABLE history (h_t_id int NOT NULL COMMENT 'lfc:v={1 + ${h101} % ${T}}',\n  h_b_id int NOT NULL COMMENT 'lfc:v={(${h101} % ${T}) DIV 10 + 1}',\n  h_a_id int NOT NULL COMMENT 'lfc:v={1 + ${h103} % ${A}}',\n  h_delta decimal(12,2) NOT NULL COMMENT 'lfc:v={x.n}', h_time datetime NOT NULL COMMENT 'lfc:v={1700000000 + x.n}', h_filler char(22),\n  FOREIGN KEY (h_t_id) REFERENCES teller (t_id), FOREIGN KEY (h_b_id) REFERENCES branch (b_id),\n  FOREIGN KEY (h_a_id) REFERENCES account (a_id)) COMMENT='lfc:rows=${H} lfc:n={CAST(h_delta * 100 AS SIGNED)} lfc:by={h_time}';\n")

TPCH = Template("CREATE TABLE region (r_regionkey int NOT NULL COMMENT 'lfc:v={x.n - 1}', r_name char(25) NOT NULL, r_comment varchar(152),\n  PRIMARY KEY (r_regionkey), CHECK (r_regionkey >= 0)) COMMENT='lfc:rows=5 lfc:n={r_regionkey + 1}';\nCREATE TABLE nation (n_nationkey int NOT NULL COMMENT 'lfc:v={x.n - 1}', n_name char(25) NOT NULL,\n  n_regionkey int NOT NULL COMMENT 'lfc:v={CASE x.n - 1 WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 3 THEN 1 WHEN 4 THEN 4 WHEN 5 THEN 0 WHEN 6 THEN 3 WHEN 7 THEN 3 WHEN 8 THEN 2 WHEN 9 THEN 2 WHEN 10 THEN 4 WHEN 11 THEN 4 WHEN 12 THEN 2 WHEN 13 THEN 4 WHEN 14 THEN 0 WHEN 15 THEN 0 WHEN 16 THEN 0 WHEN 17 THEN 1 WHEN 18 THEN 2 WHEN 19 THEN 3 WHEN 20 THEN 4 WHEN 21 THEN 2 WHEN 22 THEN 3 WHEN 23 THEN 3 ELSE 1 END}',\n  n_comment varchar(152),\n  PRIMARY KEY (n_nationkey), FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey), CHECK (n_nationkey >= 0))\n  COMMENT='lfc:rows=25 lfc:n={n_nationkey + 1}';\nCREATE TABLE supplier (s_suppkey int NOT NULL, s_name char(25) NOT NULL, s_address varchar(40) NOT NULL,\n  s_nationkey int NOT NULL COMMENT 'lfc:v={${h19} % 25}', s_phone char(15) NOT NULL,\n  s_acctbal decimal(15,2) NOT NULL COMMENT 'lfc:v={${h23} % 1099999 - 99999}', s_comment varchar(101) NOT NULL,\n  PRIMARY KEY (s_suppkey), FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey), CHECK (s_suppkey >= 0))\n  COMMENT='lfc:rows=${S}';\nCREATE TABLE part (p_partkey int NOT NULL, p_name varchar(55) NOT NULL, p_mfgr char(25) NOT NULL, p_brand char(10) NOT NULL,\n  p_type varchar(25) NOT NULL, p_size int NOT NULL COMMENT 'lfc:v={1 + ${h29} % 50}', p_container char(10) NOT NULL,\n  p_retailprice decimal(15,2) NOT NULL COMMENT 'lfc:v={90000 + (x.n DIV 10) % 20001 + 100 * (x.n % 1000)}', p_comment varchar(23) NOT NULL,\n  PRIMARY KEY (p_partkey), CHECK (p_partkey >= 0), CHECK (p_size >= 0), CHECK (p_retailprice >= 0)) COMMENT='lfc:rows=${P}';\nCREATE TABLE partsupp (ps_partkey int NOT NULL COMMENT 'lfc:v={(x.n - 1) DIV 4 + 1}',\n  ps_suppkey int NOT NULL COMMENT 'lfc:v={(((x.n - 1) DIV 4 + 1) + ((x.n - 1) % 4) * (${S} DIV 4 + ((x.n - 1) DIV 4) DIV ${S})) % ${S} + 1}',\n  ps_availqty int NOT NULL COMMENT 'lfc:v={1 + ${h31} % 9999}',\n  ps_supplycost decimal(15,2) NOT NULL COMMENT 'lfc:v={100 + ${h37} % 99901}', ps_comment varchar(199) NOT NULL,\n  PRIMARY KEY (ps_partkey, ps_suppkey), FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey),\n  FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey),\n  CHECK (ps_partkey >= 0), CHECK (ps_availqty >= 0), CHECK (ps_supplycost >= 0))\n  COMMENT='lfc:rows=${P4} lfc:n={(ps_partkey - 1) * 4 + ((ps_suppkey - 1 - ps_partkey) % ${S} + ${S}) % ${S} DIV (${S} DIV 4 + (ps_partkey - 1) DIV ${S}) + 1}';\nCREATE TABLE customer (c_custkey int NOT NULL, c_name varchar(25) NOT NULL, c_address varchar(40) NOT NULL,\n  c_nationkey int NOT NULL COMMENT 'lfc:v={${h41} % 25}', c_phone char(15) NOT NULL,\n  c_acctbal decimal(15,2) NOT NULL COMMENT 'lfc:v={${h43} % 1099999 - 99999}', c_mktsegment char(10) NOT NULL,\n  c_comment varchar(117) NOT NULL,\n  PRIMARY KEY (c_custkey), FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey), CHECK (c_custkey >= 0))\n  COMMENT='lfc:rows=${C}';\nCREATE TABLE orders (o_orderkey bigint NOT NULL COMMENT 'lfc:v={((x.n - 1) DIV 8) * 32 + (x.n - 1) % 8 + 1}',\n  o_custkey int NOT NULL COMMENT 'lfc:v={((${h47} % ${C23}) DIV 2) * 3 + ${h47} % 2 + 1}',\n  o_orderstatus char(1) NOT NULL, o_totalprice decimal(15,2) NOT NULL,\n  o_orderdate date NOT NULL COMMENT 'lfc:v={8035 + ${h11} % 2406}', o_orderpriority char(15) NOT NULL,\n  o_clerk char(15) NOT NULL, o_shippriority int NOT NULL COMMENT 'lfc:v={0}', o_comment varchar(79) NOT NULL,\n  PRIMARY KEY (o_orderkey), FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey), CHECK (o_totalprice >= 0))\n  COMMENT='lfc:rows=${O} lfc:n={((o_orderkey - 1) DIV 32) * 8 + (o_orderkey - 1) % 32 + 1}';\nCREATE TABLE lineitem (l_orderkey bigint NOT NULL COMMENT 'lfc:v={(((${ok}) - 1) DIV 8) * 32 + ((${ok}) - 1) % 8 + 1}',\n  l_partkey int NOT NULL COMMENT 'lfc:v={${lpk}}',\n  l_suppkey int NOT NULL COMMENT 'lfc:v={(${lpk} + (${h53} % 4) * (${S} DIV 4 + (${lpk} - 1) DIV ${S})) % ${S} + 1}',\n  l_linenumber int NOT NULL COMMENT 'lfc:v={(x.n - 1) % 4 + 1}',\n  l_quantity decimal(15,2) NOT NULL COMMENT 'lfc:v={100 * (1 + ${h59} % 50)}',\n  l_extendedprice decimal(15,2) NOT NULL COMMENT 'lfc:v={(1 + ${h59} % 50) * (90000 + (${lpk} DIV 10) % 20001 + 100 * (${lpk} % 1000))}',\n  l_discount decimal(15,2) NOT NULL COMMENT 'lfc:v={${h61} % 11}', l_tax decimal(15,2) NOT NULL COMMENT 'lfc:v={${h67} % 9}',\n  l_returnflag char(1) NOT NULL, l_linestatus char(1) NOT NULL,\n  l_shipdate date NOT NULL COMMENT 'lfc:v={${ship}}', l_commitdate date NOT NULL COMMENT 'lfc:v={${odate} + 30 + ${h71} % 61}',\n  l_receiptdate date NOT NULL COMMENT 'lfc:v={${ship} + 1 + ${h73} % 30}',\n  l_shipinstruct char(25) NOT NULL, l_shipmode char(10) NOT NULL, l_comment varchar(44) NOT NULL,\n  PRIMARY KEY (l_orderkey, l_linenumber), FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey),\n  FOREIGN KEY (l_partkey) REFERENCES part (p_partkey), FOREIGN KEY (l_suppkey) REFERENCES supplier (s_suppkey),\n  FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey),\n  CHECK (l_quantity >= 0), CHECK (l_extendedprice >= 0), CHECK (l_tax >= 0), CHECK (l_discount BETWEEN 0.00 AND 1.00),\n  CHECK (l_shipdate <= l_receiptdate))\n  COMMENT='lfc:rows=${O4} lfc:n={((((l_orderkey - 1) DIV 32) * 8 + (l_orderkey - 1) % 32) * 4 + l_linenumber)}';\n")


def tpc_schema(model: str, sf: float = 1, history: int | None = None) -> str:
    """The model's MySQL DDL with the lfc tags at scale factor sf."""
    sf = float(sf)
    if model == "tpcb":
        b = _int(sf); t, acc = b * 10, b * 100000
        hist = _int(float(history)) if history is not None else acc
        return TPCB.substitute(B=b, T=t, A=acc, H=hist, **{f"h{k}": h("x.n", k) for k in (101, 103)})
    if model == "tpch":
        s, p, c, o = _int(sf * 10000), _int(sf * 200000), _int(sf * 150000), _int(sf * 1500000)
        if s < 230:
            raise ValueError(f"TPC-H needs scale factor 0.03 or more (230 suppliers); got {sf:g}")
        ok = "(x.n - 1) DIV 4 + 1"                                   # a line's order number
        odate = f"8035 + {h(ok, 11)} % 2406"                       # 1992-01-01 .. 1998-08-02 (ENDDATE - 151)
        lpk = f"(1 + {h('x.n', 13)} % {p})"
        ship = f"{odate} + 1 + {h('x.n', 17)} % 121"
        hs = {f"h{k}": h("x.n", k) for k in [11, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 101, 103]}
        return TPCH.substitute(S=s, P=p, C=c, O=o, P4=p * 4, O4=o * 4, C23=c * 2 // 3, ok=ok, odate=odate, lpk=lpk,
                               ship=ship, **hs)
    raise ValueError(f"tpc_schema: {model}: tpcb or tpch (the other TPC models come from statschema's schemas)")


def main(argv: list[str] | None = None) -> int:
    a = sys.argv[1:] if argv is None else argv
    if not a:
        print(__doc__.split("\n")[0], file=sys.stderr); return 2
    try:
        sys.stdout.write(tpc_schema(a[0], a[1] if len(a) > 1 else 1, os.environ.get("HISTORY")))
    except ValueError as e:
        print(f"tpc-schema.sh: {e}" if "scale factor" in str(e) else str(e), file=sys.stderr); return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
