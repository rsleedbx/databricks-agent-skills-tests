-- tpcb-withdrawals.sql (Postgres): unusual withdrawals a model can learn. The fill's h_delta is the row number (n / 100),
-- so no amount means anything; here one bank's history decides them: a history row is a large withdrawal at a per-mille
-- rate of :base, plus :teller for a risky teller (h_t_id a multiple of :teller_every), :account for a watch-listed
-- account (h_a_id a multiple of :account_every) and :night for a night hour of h_time (from :night_from, before
-- :night_to, wrapping past midnight):
--   large: h_delta = -(:big_min .. :big_min + :big_span - 1) dollars; else +-(1 .. :normal_max) dollars
-- A row is unusual when h_delta <= -:big_min (keep :normal_max below :big_min). Then every account's, teller's and
-- branch's balance is the sum of its deltas, so the three sums match the history's, as in the spec. The row number
-- comes from h_time (1700000000 + n seconds), which the planner reads n from too. lfc_source_datagen.signals adds the
-- parameters to history's comment, after its fill tags. Deterministic (from n), so a rerun gives the same amounts.
-- Each statement updates only the rows whose value changes, so a rerun after an append touches only those.
-- psql variables, whole numbers: base teller_every teller account_every account night_from night_to night normal_max
-- big_min big_span; teller_every, account_every, normal_max, big_span at least 1.
WITH r AS (
  SELECT ctid AS rid, (EXTRACT(EPOCH FROM h_time)::bigint - 1700000000) * 2654435761 % 1000003 AS h,
         EXTRACT(HOUR FROM h_time)::int AS hr, h_t_id, h_a_id
  FROM history
),
v AS (SELECT r.rid, CASE
    WHEN r.h % 1000 < :base + CASE WHEN r.h_t_id % :teller_every = 0 THEN :teller ELSE 0 END
                            + CASE WHEN r.h_a_id % :account_every = 0 THEN :account ELSE 0 END
                            + CASE WHEN (:night_from <= :night_to AND r.hr >= :night_from AND r.hr < :night_to)
                                     OR (:night_from > :night_to AND (r.hr >= :night_from OR r.hr < :night_to))
                                   THEN :night ELSE 0 END
    THEN -(:big_min + (r.h / 7) % :big_span)
    ELSE CASE WHEN (r.h / 11) % 2 = 0 THEN 1 ELSE -1 END * (1 + (r.h / 13) % :normal_max) END AS d FROM r)
UPDATE history x SET h_delta = v.d FROM v WHERE x.ctid = v.rid AND x.h_delta IS DISTINCT FROM v.d;
WITH s AS (SELECT a.a_id AS id, COALESCE(SUM(h.h_delta), 0) AS v FROM account a LEFT JOIN history h ON h.h_a_id = a.a_id GROUP BY 1)
UPDATE account a SET a_balance = s.v FROM s WHERE a.a_id = s.id AND a.a_balance IS DISTINCT FROM s.v;
WITH s AS (SELECT t.t_id AS id, COALESCE(SUM(h.h_delta), 0) AS v FROM teller t LEFT JOIN history h ON h.h_t_id = t.t_id GROUP BY 1)
UPDATE teller t SET t_balance = s.v FROM s WHERE t.t_id = s.id AND t.t_balance IS DISTINCT FROM s.v;
WITH s AS (SELECT b.b_id AS id, COALESCE(SUM(h.h_delta), 0) AS v FROM branch b LEFT JOIN history h ON h.h_b_id = b.b_id GROUP BY 1)
UPDATE branch b SET b_balance = s.v FROM s WHERE b.b_id = s.id AND b.b_balance IS DISTINCT FROM s.v;
