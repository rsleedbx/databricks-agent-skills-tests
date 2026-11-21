-- tpcds-returns.sql (Postgres): returns a model can learn, in all three channels. The fill draws store_returns,
-- catalog_returns and web_returns on their own, so a return names no sale; here each is rebuilt from its sales, one
-- return per returned line (the same item and ticket or order, so a return joins its sale as in the spec), and whether
-- a line comes back follows one business's base rate and three causes: the item's category (Clothing or Shoes, the
-- ones that must fit, :fit more), the sale's promotion (an active discount, :promo more) and the household's income
-- (income band tiers 0..3 of the store or bill-to household, :income_step more per tier), all per mille:
--   returned when a hash of the line's keys % 1000 < :base + fit + promo + tier * :income_step
-- A line's return is a draw, so the target is a propensity: a model learns the rate per category, promotion and income,
-- not each line. Each channel draws from its own hash (the web one apart from the ship signal's). The return comes
-- :lag_min + 0 .. :lag_span - 1 days (date keys) after the sale, with part of the line's quantity and a reason that
-- follows the cause ('Did not fit' for the fit categories, 'Changed mind' for a discounted sale, else any); a catalog or
-- web return is refunded to the bill-to customer and returned by the ship-to one. lfc_source_datagen.signals adds the
-- parameters to store_returns's comment, after its fill tags. Deterministic (from the keys): the returns not wanted are
-- deleted and the missing ones inserted, so a rerun after an append changes only those.
-- psql variables, whole numbers: base fit promo income_step (per mille) lag_min lag_span (days); lag_span at least 1.
-- fragment common
r AS (   -- the reasons, numbered, and the two the causes name: the spec's 55 (reason's row_count), so reasons
  --       appended later don't move a return's reason
  SELECT r_reason_sk AS sk, r_reason_desc AS rd, ROW_NUMBER() OVER (ORDER BY r_reason_sk) - 1 AS n,
         LEAST(COUNT(*) OVER (), 55) AS cnt FROM reason),
named AS (SELECT MIN(sk) FILTER (WHERE rd = 'Did not fit' AND n < 55) AS fit, MIN(sk) FILTER (WHERE rd = 'Changed mind' AND n < 55) AS mind,
                 MAX(cnt) AS cnt FROM r),
last_day AS (SELECT MAX(d_date_sk) AS sk FROM date_dim)
-- end
-- fragment reason
CASE WHEN ret.is_fit = 1 AND named.fit IS NOT NULL THEN named.fit
            WHEN ret.is_promo = 1 AND named.mind IS NOT NULL THEN named.mind
            ELSE (SELECT r.sk FROM r WHERE r.n = (ret.h / 17) % named.cnt) END
-- end
-- fragment want
WITH {{common}},
s AS (
  SELECT ss.*, (ss.ss_item_sk::bigint * 2654435761 + ss.ss_ticket_number * 40503) % 1000003 AS h,
         COALESCE((i.i_category IN ('Clothing', 'Shoes'))::int, 0) AS is_fit,
         COALESCE((p.p_discount_active = 'Y')::int, 0) AS is_promo,   -- no promotion: none
         COALESCE((b.ib_income_band_sk - 1) / 5, 0) AS tier
  FROM store_sales ss
  JOIN item i ON i.i_item_sk = ss.ss_item_sk
  LEFT JOIN promotion p ON p.p_promo_sk = ss.ss_promo_sk
  LEFT JOIN household_demographics hd ON hd.hd_demo_sk = ss.ss_hdemo_sk
  LEFT JOIN income_band b ON b.ib_income_band_sk = hd.hd_income_band_sk
),
ret AS (
  SELECT s.*, 1 + (s.h / 7) % GREATEST(COALESCE(s.ss_quantity, 1), 1) AS qty FROM s
  WHERE s.h % 1000 < :base + s.is_fit * :fit + s.is_promo * :promo + s.tier * :income_step
)
SELECT LEAST(ret.ss_sold_date_sk + :lag_min + (ret.h / 13) % :lag_span, last_day.sk) AS sr_returned_date_sk,
       ret.ss_sold_time_sk AS sr_return_time_sk, ret.ss_item_sk AS sr_item_sk, ret.ss_customer_sk AS sr_customer_sk,
       ret.ss_cdemo_sk AS sr_cdemo_sk, ret.ss_hdemo_sk AS sr_hdemo_sk, ret.ss_addr_sk AS sr_addr_sk,
       ret.ss_store_sk AS sr_store_sk,
       {{reason}} AS sr_reason_sk,
       ret.ss_ticket_number AS sr_ticket_number, ret.qty AS sr_return_quantity,
       ROUND(COALESCE(ret.ss_sales_price, 0) * ret.qty, 2) AS sr_return_amt,
       ROUND(COALESCE(ret.ss_ext_tax, 0) * ret.qty / GREATEST(COALESCE(ret.ss_quantity, 1), 1), 2) AS sr_return_tax,
       ROUND(COALESCE(ret.ss_sales_price, 0) * ret.qty, 2)
         + ROUND(COALESCE(ret.ss_ext_tax, 0) * ret.qty / GREATEST(COALESCE(ret.ss_quantity, 1), 1), 2) AS sr_return_amt_inc_tax,
       0.50 + (ret.h / 19) % 50 AS sr_fee, 0 AS sr_return_ship_cost, ROUND(COALESCE(ret.ss_sales_price, 0) * ret.qty, 2) AS sr_refunded_cash,
       0 AS sr_reversed_charge, 0 AS sr_store_credit, 0.50 + (ret.h / 19) % 50 AS sr_net_loss
FROM ret CROSS JOIN named CROSS JOIN last_day
-- end
DELETE FROM store_returns sr WHERE NOT EXISTS (SELECT 1 FROM ({{want}}) w
  WHERE w.sr_item_sk = sr.sr_item_sk AND w.sr_ticket_number = sr.sr_ticket_number AND ROW(w.*) IS NOT DISTINCT FROM ROW(sr.*));
INSERT INTO store_returns
SELECT w.* FROM ({{want}}) w
WHERE NOT EXISTS (SELECT 1 FROM store_returns sr WHERE sr.sr_item_sk = w.sr_item_sk AND sr.sr_ticket_number = w.sr_ticket_number);
-- fragment want_cr
WITH {{common}},
s AS (
  SELECT cs.*, (cs.cs_item_sk::bigint * 3266489917 + cs.cs_order_number * 668265263) % 1000003 AS h,
         COALESCE((i.i_category IN ('Clothing', 'Shoes'))::int, 0) AS is_fit,
         COALESCE((p.p_discount_active = 'Y')::int, 0) AS is_promo,   -- no promotion: none
         COALESCE((b.ib_income_band_sk - 1) / 5, 0) AS tier
  FROM catalog_sales cs
  JOIN item i ON i.i_item_sk = cs.cs_item_sk
  LEFT JOIN promotion p ON p.p_promo_sk = cs.cs_promo_sk
  LEFT JOIN household_demographics hd ON hd.hd_demo_sk = cs.cs_bill_hdemo_sk
  LEFT JOIN income_band b ON b.ib_income_band_sk = hd.hd_income_band_sk
),
ret AS (
  SELECT s.*, 1 + (s.h / 7) % GREATEST(COALESCE(s.cs_quantity, 1), 1) AS qty FROM s
  WHERE s.h % 1000 < :base + s.is_fit * :fit + s.is_promo * :promo + s.tier * :income_step
)
SELECT LEAST(ret.cs_sold_date_sk + :lag_min + (ret.h / 13) % :lag_span, last_day.sk) AS cr_returned_date_sk,
       ret.cs_sold_time_sk AS cr_returned_time_sk, ret.cs_item_sk AS cr_item_sk,
       ret.cs_bill_customer_sk AS cr_refunded_customer_sk, ret.cs_bill_cdemo_sk AS cr_refunded_cdemo_sk,
       ret.cs_bill_hdemo_sk AS cr_refunded_hdemo_sk, ret.cs_bill_addr_sk AS cr_refunded_addr_sk,
       ret.cs_ship_customer_sk AS cr_returning_customer_sk, ret.cs_ship_cdemo_sk AS cr_returning_cdemo_sk,
       ret.cs_ship_hdemo_sk AS cr_returning_hdemo_sk, ret.cs_ship_addr_sk AS cr_returning_addr_sk,
       ret.cs_call_center_sk AS cr_call_center_sk, ret.cs_catalog_page_sk AS cr_catalog_page_sk,
       ret.cs_ship_mode_sk AS cr_ship_mode_sk, ret.cs_warehouse_sk AS cr_warehouse_sk,
       {{reason}} AS cr_reason_sk,
       ret.cs_order_number AS cr_order_number, ret.qty AS cr_return_quantity,
       ROUND(COALESCE(ret.cs_sales_price, 0) * ret.qty, 2) AS cr_return_amount,
       ROUND(COALESCE(ret.cs_ext_tax, 0) * ret.qty / GREATEST(COALESCE(ret.cs_quantity, 1), 1), 2) AS cr_return_tax,
       ROUND(COALESCE(ret.cs_sales_price, 0) * ret.qty, 2)
         + ROUND(COALESCE(ret.cs_ext_tax, 0) * ret.qty / GREATEST(COALESCE(ret.cs_quantity, 1), 1), 2) AS cr_return_amt_inc_tax,
       0.50 + (ret.h / 19) % 50 AS cr_fee, 0 AS cr_return_ship_cost, ROUND(COALESCE(ret.cs_sales_price, 0) * ret.qty, 2) AS cr_refunded_cash,
       0 AS cr_reversed_charge, 0 AS cr_store_credit, 0.50 + (ret.h / 19) % 50 AS cr_net_loss
FROM ret CROSS JOIN named CROSS JOIN last_day
-- end
-- fragment want_wr
WITH {{common}},
s AS (
  SELECT ws.*, (ws.ws_item_sk::bigint * 2246822519 + ws.ws_order_number * 374761393) % 1000003 AS h,
         COALESCE((i.i_category IN ('Clothing', 'Shoes'))::int, 0) AS is_fit,
         COALESCE((p.p_discount_active = 'Y')::int, 0) AS is_promo,   -- no promotion: none
         COALESCE((b.ib_income_band_sk - 1) / 5, 0) AS tier
  FROM web_sales ws
  JOIN item i ON i.i_item_sk = ws.ws_item_sk
  LEFT JOIN promotion p ON p.p_promo_sk = ws.ws_promo_sk
  LEFT JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_bill_hdemo_sk
  LEFT JOIN income_band b ON b.ib_income_band_sk = hd.hd_income_band_sk
),
ret AS (
  SELECT s.*, 1 + (s.h / 7) % GREATEST(COALESCE(s.ws_quantity, 1), 1) AS qty FROM s
  WHERE s.h % 1000 < :base + s.is_fit * :fit + s.is_promo * :promo + s.tier * :income_step
)
SELECT LEAST(ret.ws_sold_date_sk + :lag_min + (ret.h / 13) % :lag_span, last_day.sk) AS wr_returned_date_sk,
       ret.ws_sold_time_sk AS wr_returned_time_sk, ret.ws_item_sk AS wr_item_sk,
       ret.ws_bill_customer_sk AS wr_refunded_customer_sk, ret.ws_bill_cdemo_sk AS wr_refunded_cdemo_sk,
       ret.ws_bill_hdemo_sk AS wr_refunded_hdemo_sk, ret.ws_bill_addr_sk AS wr_refunded_addr_sk,
       ret.ws_ship_customer_sk AS wr_returning_customer_sk, ret.ws_ship_cdemo_sk AS wr_returning_cdemo_sk,
       ret.ws_ship_hdemo_sk AS wr_returning_hdemo_sk, ret.ws_ship_addr_sk AS wr_returning_addr_sk,
       ret.ws_web_page_sk AS wr_web_page_sk,
       {{reason}} AS wr_reason_sk,
       ret.ws_order_number AS wr_order_number, ret.qty AS wr_return_quantity,
       ROUND(COALESCE(ret.ws_sales_price, 0) * ret.qty, 2) AS wr_return_amt,
       ROUND(COALESCE(ret.ws_ext_tax, 0) * ret.qty / GREATEST(COALESCE(ret.ws_quantity, 1), 1), 2) AS wr_return_tax,
       ROUND(COALESCE(ret.ws_sales_price, 0) * ret.qty, 2)
         + ROUND(COALESCE(ret.ws_ext_tax, 0) * ret.qty / GREATEST(COALESCE(ret.ws_quantity, 1), 1), 2) AS wr_return_amt_inc_tax,
       0.50 + (ret.h / 19) % 50 AS wr_fee, 0 AS wr_return_ship_cost, ROUND(COALESCE(ret.ws_sales_price, 0) * ret.qty, 2) AS wr_refunded_cash,
       0 AS wr_reversed_charge, 0 AS wr_account_credit, 0.50 + (ret.h / 19) % 50 AS wr_net_loss
FROM ret CROSS JOIN named CROSS JOIN last_day
-- end
DELETE FROM catalog_returns x WHERE NOT EXISTS (SELECT 1 FROM ({{want_cr}}) w
  WHERE w.cr_item_sk = x.cr_item_sk AND w.cr_order_number = x.cr_order_number AND ROW(w.*) IS NOT DISTINCT FROM ROW(x.*));
INSERT INTO catalog_returns
SELECT w.* FROM ({{want_cr}}) w
WHERE NOT EXISTS (SELECT 1 FROM catalog_returns x WHERE x.cr_item_sk = w.cr_item_sk AND x.cr_order_number = w.cr_order_number);
DELETE FROM web_returns x WHERE NOT EXISTS (SELECT 1 FROM ({{want_wr}}) w
  WHERE w.wr_item_sk = x.wr_item_sk AND w.wr_order_number = x.wr_order_number AND ROW(w.*) IS NOT DISTINCT FROM ROW(x.*));
INSERT INTO web_returns
SELECT w.* FROM ({{want_wr}}) w
WHERE NOT EXISTS (SELECT 1 FROM web_returns x WHERE x.wr_item_sk = w.wr_item_sk AND x.wr_order_number = w.wr_order_number);
