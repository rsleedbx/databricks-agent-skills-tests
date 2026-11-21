-- gold (TPC-B pack): what a fraud or operations analyst reads, across every LOB.
--   transactions: each transaction with its account's balance; unusual when it withdraws large_withdrawal or more
--   unusual_by_hour: per LOB, branch and hour, how many transactions and the unusual share
--   lob_summary: one row per LOB (hundreds of LOBs read as one table): transactions, unusual share, and whether the
--                balances still equal the sum of the history (TPC-B's consistency rule)

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.transactions
COMMENT 'Every LOB''s transactions with the account''s balance; unusual: a withdrawal of large_withdrawal dollars or more'
CLUSTER BY (lob, hour)
AS SELECT
  x.lob, x.at, x.hour, x.branch_id, x.teller_id, x.account_id, x.amount,
  x.amount < 0 AS withdrawal,
  x.amount <= -${large_withdrawal} AS unusual,
  a.balance AS account_balance
FROM silver_transactions AS x
JOIN silver_accounts AS a ON a.lob = x.lob AND a.account_id = x.account_id;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.unusual_by_hour
COMMENT 'Per LOB, branch and hour of day: transactions, unusual ones and their share'
AS SELECT
  lob, branch_id, hour,
  COUNT(*) AS transactions,
  SUM(CASE WHEN unusual THEN 1 ELSE 0 END) AS unusual,
  ROUND(AVG(CASE WHEN unusual THEN 1.0 ELSE 0.0 END), 4) AS unusual_share
FROM ${catalog}.${gold_schema}.transactions
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.lob_summary
COMMENT 'One row per LOB: transactions, the unusual share, and the gap between account balances and the history''s sum'
AS SELECT
  x.lob,
  COUNT(*) AS transactions,
  ROUND(AVG(CASE WHEN x.amount <= -${large_withdrawal} THEN 1.0 ELSE 0.0 END), 4) AS unusual_share,
  ROUND(SUM(x.amount), 2) AS history_sum,
  ROUND(MAX(b.balance), 2) AS balance_sum,
  ROUND(MAX(b.balance) - SUM(x.amount), 2) AS balance_gap
FROM silver_transactions AS x
JOIN (SELECT lob, SUM(balance) AS balance FROM silver_accounts GROUP BY lob) AS b ON b.lob = x.lob
GROUP BY x.lob;
