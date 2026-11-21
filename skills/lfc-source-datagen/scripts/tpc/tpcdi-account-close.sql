-- tpcdi-account-close.sql (Postgres): accounts that close in a way a model can learn. The fill draws an account's
-- Status (6% Inactive) and every cash balance on its own, so nothing predicts a closing; here one broker-dealer's
-- book decides them. Each account's cash moves by its own drift, -:drift_span .. :drift_span dollars from one balance
-- row to the next (by date); it is falling when the drift is below -:fall_at and it has two or more rows. An account
-- closes (Status Inactive) at a per-mille rate of :base, plus (Tier - 1) * :tier_step for its customer's tier (tier 3
-- closes most), :broker for a slow broker (SK_BrokerID a multiple of :broker_every) and :falling for a falling balance.
-- lfc_source_datagen.signals adds the parameters to DimAccount's comment, after its fill tags. Deterministic (from the
-- keys), so a rerun gives the same values. The closing draw has its own hash, so the cash shows nothing of it.
-- Each statement updates only the rows whose value changes, so a rerun after an append touches only those.
-- psql variables, whole numbers: base tier_step broker_every broker falling drift_span fall_at; broker_every and
-- drift_span at least 1.
WITH c AS (
  SELECT "SK_AccountID" AS a, "SK_DateID" AS d,
         1000 + ("SK_AccountID" * 2654435761 % 1000003) % 50000
         + (ROW_NUMBER() OVER (PARTITION BY "SK_AccountID" ORDER BY "SK_DateID") - 1)
           * (("SK_AccountID" * 40503 % 1000003) % (2 * :drift_span + 1) - :drift_span) AS v
  FROM "FactCashBalances"
)
UPDATE "FactCashBalances" f SET "Cash" = c.v
FROM c WHERE f."SK_AccountID" = c.a AND f."SK_DateID" = c.d AND f."Cash" IS DISTINCT FROM c.v;
WITH fl AS (   -- falling accounts: a drift below -fall_at, seen over two or more balances
  SELECT "SK_AccountID" AS id FROM "FactCashBalances" GROUP BY 1
  HAVING COUNT(*) > 1 AND ("SK_AccountID" * 40503 % 1000003) % (2 * :drift_span + 1) - :drift_span < -:fall_at
),
r AS (
  SELECT a."SK_AccountID" AS id, (a."SK_AccountID" * 2246822519 % 999983) % 1000 AS h,   -- its own hash: the cash
          --                                                                         doesn't give the draw away
         COALESCE(cu."Tier", 1) AS tier, a."SK_BrokerID" % :broker_every = 0 AS slow, fl.id IS NOT NULL AS falling
  FROM "DimAccount" a
  LEFT JOIN "DimCustomer" cu ON cu."SK_CustomerID" = a."SK_CustomerID"
  LEFT JOIN fl ON fl.id = a."SK_AccountID"
)
, s AS (SELECT r.id, CASE
    WHEN r.h < :base + (r.tier - 1) * :tier_step + CASE WHEN r.slow THEN :broker ELSE 0 END
                     + CASE WHEN r.falling THEN :falling ELSE 0 END THEN 'Inactive' ELSE 'Active' END AS st FROM r)
UPDATE "DimAccount" x SET "Status" = s.st FROM s WHERE x."SK_AccountID" = s.id AND x."Status" IS DISTINCT FROM s.st;
