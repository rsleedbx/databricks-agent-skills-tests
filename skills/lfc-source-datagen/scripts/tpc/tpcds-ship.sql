-- tpcds-ship.sql (Postgres): web ship dates a model can learn. The fill draws ws_ship_date_sk on its own, anywhere in
-- date_dim, so a line can ship before it sells; here a line ships after its sale by one business's handling and three
-- causes: the ship mode's tier (sm_type: OVERNIGHT, NEXT DAY AIR, EXPRESS 0; TWO DAY, PRIORITY 1; GROUND, SURFACE,
-- LIBRARY 2, times :mode_step days), a slow warehouse (one in :slow_every, :slow_wh days more) and a bulk line (a
-- quantity of :bulk_qty or more, :bulk days more), plus a jitter:
--   ship = sold + :handle_min + 0 .. :handle_span - 1 + tier + slow + bulk + 0 .. :jitter - 1   (date keys: days)
-- A line is late when ship - sold > :promise days (the comment on web_sales keeps the parameters, so a summary or a
-- report reads the promise from the data). Deterministic (from the keys), so a rerun gives the same dates.
-- psql variables, whole numbers: promise handle_min handle_span mode_step slow_every slow_wh bulk_qty bulk jitter
-- (days but slow_every and bulk_qty). handle_span, slow_every and jitter at least 1.
WITH last_day AS (SELECT MAX(d_date_sk) AS sk FROM date_dim),
w AS (
  SELECT ws.ws_item_sk AS item, ws.ws_order_number AS ord,
         (:handle_min + ((ws.ws_item_sk::bigint * 2654435761 + ws.ws_order_number * 40503) % 1000003) % :handle_span
          + CASE WHEN sm.sm_type IN ('OVERNIGHT', 'NEXT DAY AIR', 'EXPRESS') THEN 0
                 WHEN sm.sm_type IN ('TWO DAY', 'PRIORITY') THEN 1 ELSE 2 END * :mode_step
          + CASE WHEN ws.ws_warehouse_sk % :slow_every = 0 THEN :slow_wh ELSE 0 END
          + CASE WHEN ws.ws_quantity >= :bulk_qty THEN :bulk ELSE 0 END
          + ((ws.ws_item_sk::bigint * 40503 + ws.ws_order_number * 2654435761) % 1000003) % :jitter)::int AS days
  FROM web_sales ws LEFT JOIN ship_mode sm ON sm.sm_ship_mode_sk = ws.ws_ship_mode_sk
  WHERE ws.ws_sold_date_sk IS NOT NULL
)
UPDATE web_sales ws SET ws_ship_date_sk = LEAST(ws.ws_sold_date_sk + w.days, last_day.sk)
FROM w CROSS JOIN last_day WHERE ws.ws_item_sk = w.item AND ws.ws_order_number = w.ord;
COMMENT ON TABLE web_sales IS :'signal';
