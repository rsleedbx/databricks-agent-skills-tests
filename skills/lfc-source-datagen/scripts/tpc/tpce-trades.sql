-- tpce-trades.sql (Postgres): trades that fail, cancelled or completed late, in a way a model can learn. The fill draws
-- a trade's status (all CMPT), its bid and its history times on their own; here one brokerage's handling decides them:
--   a limit order (TLB, TLS) bids 0 .. :spread per mille away from its security's last price (below it to buy, above
--   it to sell); one more than :far per mille away is cancelled :cancel_far percent of the time, any trade
--   :cancel_base per mille of the time;
--   the others complete after :handle_min + 0 .. :handle_span - 1 minutes, plus :limit_wait for a limit order, :slow_broker
--   for a slow broker (one in :slow_every, ca_b_id), (c_tier - 1) * :tier_step for the customer's tier, :margin for a
--   trade not paid in cash (t_is_cash 0), and 0 .. :jitter - 1.
-- A trade fails when it's cancelled or completes more than :promise minutes after it was submitted. trade_history is
-- rebuilt to match: SBMT at t_dts, PNDG a second later for a limit order, then CMPT or CNCL; settlement keeps the
-- completed trades, its cash type from t_is_cash and its due date two days after completion. The comment on trade keeps
-- the parameters. Deterministic (from the keys), so a rerun gives the same values.
-- psql variables, whole numbers: promise handle_min handle_span limit_wait slow_every slow_broker tier_step margin jitter
-- spread far cancel_far cancel_base; handle_span, slow_every, jitter at least 1.
WITH t AS (
  SELECT t.t_id, (t.t_id * 2654435761) % 1000003 AS h, t.t_tt_id IN ('TLB', 'TLS') AS lim, lt.lt_price AS px
  FROM trade t JOIN last_trade lt ON lt.lt_s_symb = t.t_s_symb
)
UPDATE trade x SET
  t_bid_price = CASE WHEN t.lim THEN ROUND(t.px * (1000 + CASE x.t_tt_id WHEN 'TLB' THEN -1 ELSE 1 END
                                                          * ((t.h / 7) % (:spread + 1))) / 1000.0, 2)
                     ELSE x.t_bid_price END,
  t_st_id = CASE WHEN (t.lim AND (t.h / 7) % (:spread + 1) > :far AND (t.h / 11) % 100 < :cancel_far)
                      OR (t.h / 13) % 1000 < :cancel_base THEN 'CNCL' ELSE 'CMPT' END
FROM t WHERE x.t_id = t.t_id;
DELETE FROM trade_history;
WITH m AS (
  SELECT t.t_id, t.t_dts, t.t_st_id, t.t_tt_id IN ('TLB', 'TLS') AS lim,
         GREATEST(1, :handle_min + ((t.t_id * 2654435761) % 1000003 / 17) % :handle_span
           + CASE WHEN t.t_tt_id IN ('TLB', 'TLS') THEN :limit_wait ELSE 0 END
           + CASE WHEN ca.ca_b_id % :slow_every = 0 THEN :slow_broker ELSE 0 END
           + (COALESCE(c.c_tier, 1) - 1) * :tier_step
           + CASE WHEN t.t_is_cash = 0 THEN :margin ELSE 0 END
           + ((t.t_id * 2654435761) % 1000003 / 19) % :jitter) AS mins
  FROM trade t JOIN customer_account ca ON ca.ca_id = t.t_ca_id LEFT JOIN customer c ON c.c_id = ca.ca_c_id
)
INSERT INTO trade_history (th_t_id, th_dts, th_st_id)
SELECT t_id, t_dts, 'SBMT' FROM m
UNION ALL SELECT t_id, t_dts + interval '1 second', 'PNDG' FROM m WHERE lim
UNION ALL SELECT t_id, t_dts + mins * interval '1 minute' + interval '2 seconds', t_st_id FROM m;
DELETE FROM settlement s USING trade t WHERE t.t_id = s.se_t_id AND t.t_st_id = 'CNCL';
UPDATE settlement s SET se_cash_type = CASE WHEN t.t_is_cash = 1 THEN 'Cash Account' ELSE 'Margin' END,
       se_cash_due_date = (h.th_dts + interval '2 days')::date
FROM trade t JOIN trade_history h ON h.th_t_id = t.t_id AND h.th_st_id = 'CMPT'
WHERE t.t_id = s.se_t_id;
COMMENT ON TABLE trade IS :'signal';
