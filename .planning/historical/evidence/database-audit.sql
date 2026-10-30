-- Connection credentials are loaded in memory from the existing secret store.
-- Run through a connection with default_transaction_read_only=on and a timeout.
-- This query contains no connection information and performs no writes.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT json_build_object(
  'raw_formats', (SELECT json_agg(r ORDER BY id) FROM catalog.raw_formats r),
  'normalization_rules', (SELECT json_agg(n ORDER BY id) FROM catalog.normalization_rules n),
  'exchanges', (SELECT json_agg(e ORDER BY id) FROM catalog.exchanges e)
);
COMMIT;
