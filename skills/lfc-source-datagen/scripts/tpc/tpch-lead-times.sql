-- tpch-lead-times.sql (Postgres): lead times on TPC-H's lines that a model can learn. The spec's dates make lateness
-- random (commit 30-90 days and receipt 1-30 days after ship, drawn apart); here a line's dates follow one business's
-- committed lead time and three causes: the supplier's nation (one in :slow_every nations is slow, :slow_nation days
-- more handling), the ship mode (transit days: :air for AIR and REG AIR, :truck for TRUCK, MAIL and FOB, :sea for RAIL
-- and SHIP), and a jitter. Run in the workload's schema after tpch-labels.sql (it reads the spec's ship modes).
--   commit  = order + :commit                                     (the promise to the customer)
--   ship    = order + :handle_min + 0 .. :handle_span - 1 + slow   (handling at the supplier)
--   receipt = ship + transit(ship mode) + 0 .. :jitter - 1          (transit)
-- A line is late when receipt > commit. Deterministic (from the keys), so a rerun gives the same dates. The spec's date
-- rules (tpch-verify.sql) no longer hold afterwards: check them before.
-- psql variables, all whole numbers (days, except slow_every): commit handle_min handle_span slow_nation slow_every air
-- truck sea jitter. handle_span and jitter at least 1.
WITH d AS (
  SELECT l.l_orderkey AS k, l.l_linenumber AS n, o.o_orderdate AS od,
         (:handle_min + (l.l_orderkey * 7 + l.l_linenumber * 13) % :handle_span
          + CASE WHEN s.s_nationkey % :slow_every = 0 THEN :slow_nation ELSE 0 END)::int AS ship_d,
         (CASE rtrim(l.l_shipmode) WHEN 'AIR' THEN :air WHEN 'REG AIR' THEN :air WHEN 'RAIL' THEN :sea WHEN 'SHIP' THEN :sea ELSE :truck END
          + (l.l_orderkey * 11 + l.l_linenumber * 3) % :jitter)::int AS transit_d
  FROM lineitem l JOIN orders o ON o.o_orderkey = l.l_orderkey JOIN supplier s ON s.s_suppkey = l.l_suppkey
)
UPDATE lineitem l SET l_commitdate = d.od + :commit, l_shipdate = d.od + d.ship_d, l_receiptdate = d.od + d.ship_d + d.transit_d
FROM d WHERE l.l_orderkey = d.k AND l.l_linenumber = d.n;
