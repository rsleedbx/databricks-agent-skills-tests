-- tpcdi-account-close.sql (Postgres): accounts that close in a way a model can learn. The fill draws an account's
-- Status (6% Inactive) and every cash balance on its own, so nothing predicts a closing; here one broker-dealer's
-- book decides them. Each account's cash moves by its own drift, -:drift_span .. :drift_span dollars from one balance
-- row to the next (by date); it is falling when the drift is below -:fall_at and it has two or more rows. An account
-- closes (Status Inactive) at a per-mille rate of :base, plus (Tier - 1) * :tier_step for its customer's tier (tier 3
-- closes most), :broker for a slow broker (SK_BrokerID a multiple of :broker_every) and :falling for a falling balance.
-- lfc_source_datagen.signals adds the parameters to DimAccount's comment, after its fill tags. Deterministic (from the
-- keys), so a rerun gives the same values.
-- psql variables, whole numbers: base tier_step broker_every broker falling drift_span fall_at; broker_every and
-- drift_span at least 1.
WITH c AS (
  SELECT "SK_AccountID" AS a, "SK_DateID" AS d,
         ROW_NUMBER() OVER (PARTITION BY "SK_AccountID" ORDER BY "SK_DateID") - 1 AS k
  FROM "FactCashBalances"
)
UPDATE "FactCashBalances" f SET "Cash" = 1000 + (f."SK_AccountID" * 2654435761 % 1000003) % 50000
       + c.k * ((f."SK_AccountID" * 40503 % 1000003) % (2 * :drift_span + 1) - :drift_span)
FROM c WHERE f."SK_AccountID" = c.a AND f."SK_DateID" = c.d;
WITH fl AS (   -- falling accounts: a drift below -fall_at, seen over two or more balances
  SELECT "SK_AccountID" AS id FROM "FactCashBalances" GROUP BY 1
  HAVING COUNT(*) > 1 AND ("SK_AccountID" * 40503 % 1000003) % (2 * :drift_span + 1) - :drift_span < -:fall_at
),
r AS (
  SELECT a."SK_AccountID" AS id, (a."SK_AccountID" * 2654435761 % 1000003 / 7) % 1000 AS h,
         COALESCE(cu."Tier", 1) AS tier, a."SK_BrokerID" % :broker_every = 0 AS slow, fl.id IS NOT NULL AS falling
  FROM "DimAccount" a
  LEFT JOIN "DimCustomer" cu ON cu."SK_CustomerID" = a."SK_CustomerID"
  LEFT JOIN fl ON fl.id = a."SK_AccountID"
)
UPDATE "DimAccount" x SET "Status" = CASE
    WHEN r.h < :base + (r.tier - 1) * :tier_step + CASE WHEN r.slow THEN :broker ELSE 0 END
                     + CASE WHEN r.falling THEN :falling ELSE 0 END THEN 'Inactive' ELSE 'Active' END
FROM r WHERE x."SK_AccountID" = r.id;
