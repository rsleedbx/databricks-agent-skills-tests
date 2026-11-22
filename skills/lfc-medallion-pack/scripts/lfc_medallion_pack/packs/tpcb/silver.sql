-- silver (TPC-B pack): the stacked n:1 tables (all_<table>, every LOB's rows with lob) cleaned and named, one view per
-- business entity. Keys repeat across LOBs (every bank's ids start at 1), so every key and join includes lob.

CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  CONSTRAINT known_lob EXPECT (lob IS NOT NULL),
  CONSTRAINT has_amount EXPECT (amount IS NOT NULL)
)
COMMENT 'Every LOB''s teller transactions (TPC-B history); one row per (lob, at)'
CLUSTER BY (lob, at)
AS SELECT
  h.lob,
  h.h_time AS at,
  HOUR(h.h_time) AS hour,
  h.h_b_id AS branch_id,
  h.h_t_id AS teller_id,
  h.h_a_id AS account_id,
  CAST(h.h_delta AS DOUBLE) AS amount
FROM all_history AS h;

CREATE OR REFRESH MATERIALIZED VIEW silver_accounts
COMMENT 'Every LOB''s accounts with their branch and balance; one row per (lob, account_id)'
AS SELECT
  a.lob,
  a.a_id AS account_id,
  a.a_b_id AS branch_id,
  CAST(a.a_balance AS DOUBLE) AS balance
FROM all_account AS a;

CREATE OR REFRESH MATERIALIZED VIEW silver_tellers
COMMENT 'Every LOB''s tellers with their branch and balance; one row per (lob, teller_id)'
AS SELECT
  t.lob,
  t.t_id AS teller_id,
  t.t_b_id AS branch_id,
  CAST(t.t_balance AS DOUBLE) AS balance
FROM all_teller AS t;

CREATE OR REFRESH MATERIALIZED VIEW silver_branches
COMMENT 'Every LOB''s branches and their balance; one row per (lob, branch_id)'
AS SELECT
  b.lob,
  b.b_id AS branch_id,
  CAST(b.b_balance AS DOUBLE) AS balance
FROM all_branch AS b;
