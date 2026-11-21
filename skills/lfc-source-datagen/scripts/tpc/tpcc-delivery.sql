-- tpcc-delivery.sql (Postgres): delivery times on TPC-C's orders that a model can learn. In the spec a delivered line's
-- ol_delivery_d is its order's o_entry_d, so there is no lateness at all; here an order's delivery follows one
-- business's handling and four causes: a slow district (one in :slow_every districts, :slow_district days more), the
-- carrier's tier (o_carrier_id 1..10 in three tiers, 0, 1 or 2 times :carrier_step days), a credit hold (a customer
-- with bad credit, c_credit BC, waits :credit_hold days), and a backorder (a line whose item has under :low_stock in
-- stock at its supplying warehouse holds the order :backorder days), plus a jitter.
--   delivery = entry + :handle_min + 0 .. :handle_span - 1 + slow + tier + hold + backorder + 0 .. :jitter - 1
-- An order is late when delivery - entry > :promise days (TPC-C has no promised date: lfc_source_datagen.signals adds
-- the parameters to order_line's comment, after its fill tags, so a summary or a report reads them from the data).
-- Orders still in new_order are open: no carrier, no delivery, as in the spec. Deterministic (from the keys), so a
-- rerun gives the same dates.
-- Each statement updates only the rows whose value changes, so a rerun after an append touches only those.
-- psql variables, all whole numbers: promise handle_min handle_span slow_every slow_district carrier_step credit_hold
-- low_stock backorder jitter (days but slow_every and low_stock). handle_span, slow_every and jitter at least 1.
UPDATE orders o SET o_carrier_id = NULL
WHERE o.o_carrier_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM new_order n WHERE n.no_w_id = o.o_w_id AND n.no_d_id = o.o_d_id AND n.no_o_id = o.o_id);
UPDATE order_line ol SET ol_delivery_d = NULL
WHERE ol.ol_delivery_d IS NOT NULL
  AND EXISTS (SELECT 1 FROM new_order n WHERE n.no_w_id = ol.ol_w_id AND n.no_d_id = ol.ol_d_id AND n.no_o_id = ol.ol_o_id);
WITH short AS (   -- orders with a line short of stock at its supplying warehouse
  SELECT DISTINCT ol.ol_w_id AS w, ol.ol_d_id AS d, ol.ol_o_id AS o
  FROM order_line ol JOIN stock s ON s.s_w_id = ol.ol_supply_w_id AND s.s_i_id = ol.ol_i_id
  WHERE s.s_quantity < :low_stock),
d AS (   -- delivered orders (open ones have no carrier now)
  SELECT o.o_w_id AS w, o.o_d_id AS dd, o.o_id AS oid, o.o_entry_d AS e,
         (:handle_min + (o.o_id * 7 + o.o_d_id * 13) % :handle_span
          + CASE WHEN o.o_d_id % :slow_every = 0 THEN :slow_district ELSE 0 END
          + ((o.o_carrier_id - 1) % 3) * :carrier_step
          + CASE WHEN rtrim(c.c_credit) = 'BC' THEN :credit_hold ELSE 0 END
          + CASE WHEN short.o IS NOT NULL THEN :backorder ELSE 0 END
          + (o.o_id * 11 + o.o_d_id * 3) % :jitter)::int AS days
  FROM orders o
  JOIN customer c ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
  LEFT JOIN short ON short.w = o.o_w_id AND short.d = o.o_d_id AND short.o = o.o_id
  WHERE o.o_carrier_id IS NOT NULL
)
UPDATE order_line ol SET ol_delivery_d = d.e + d.days * interval '1 day'
FROM d WHERE ol.ol_w_id = d.w AND ol.ol_d_id = d.dd AND ol.ol_o_id = d.oid
  AND ol.ol_delivery_d IS DISTINCT FROM d.e + d.days * interval '1 day';
