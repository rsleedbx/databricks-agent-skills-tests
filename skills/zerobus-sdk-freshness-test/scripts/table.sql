-- The target table for producer.py. Zerobus writes only to an existing managed Delta table.
-- Replace <catalog>.<schema>.<table>, then grant the producer's service principal on it
-- (zerobus-workspace-setup grant.sh: USE CATALOG, USE SCHEMA, SELECT and MODIFY, each directly on its securable).
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table> (
  event_id         STRING    COMMENT 'UUID per event; a duplicate keeps the same id',
  observation_time TIMESTAMP COMMENT 'When the sensor read the value',
  producer_time    TIMESTAMP COMMENT 'When producer.py handed the event to the SDK; freshness is measured from it',
  tag_id           STRING,
  value            DOUBLE    COMMENT 'NULL for state and fault events',
  value_text       STRING    COMMENT 'State or fault code',
  quality          STRING    COMMENT 'GOOD | UNCERTAIN | BAD',
  asset_id         STRING,
  asset_type       STRING,
  system           STRING,
  measurement      STRING,
  unit             STRING,
  eng_low          DOUBLE,
  eng_high         DOUBLE,
  producer_id      STRING    COMMENT 'Which stream sent it: <producer-id>-<stream>'
);
