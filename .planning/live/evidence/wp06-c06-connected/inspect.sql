BEGIN READ ONLY;
SELECT jsonb_build_object(
  'database', current_database(), 'role', current_user,
  'transaction_read_only', current_setting('transaction_read_only'),
  'columns', (SELECT jsonb_agg(to_jsonb(c) ORDER BY table_name, ordinal_position) FROM
    (SELECT table_name, column_name, ordinal_position, data_type, udt_name, is_nullable, numeric_precision, numeric_scale
     FROM information_schema.columns WHERE table_schema = 'live_canonical') c),
  'constraints', (SELECT jsonb_agg(to_jsonb(c) ORDER BY table_name, name) FROM
    (SELECT cl.relname AS table_name, co.conname AS name, pg_get_constraintdef(co.oid) AS definition
     FROM pg_constraint co JOIN pg_class cl ON cl.oid = co.conrelid JOIN pg_namespace n ON n.oid = cl.relnamespace
     WHERE n.nspname = 'live_canonical') c),
  'permissions', (SELECT jsonb_agg(jsonb_build_object('table', table_name,
     'select', has_table_privilege(current_user, format('%I.%I', table_schema, table_name), 'SELECT')) ORDER BY table_name)
     FROM information_schema.tables WHERE table_schema = 'live_canonical')
);
-- Typed expression probes only: these neither insert data nor invoke triggers.
SELECT jsonb_build_object(
 'u64_max', (18446744073709551615::numeric(20,0))::text,
 'i64_max', (9223372036854775807::numeric(20,0))::text,
 'exact_decimal', ('123456789.12345678901234567890'::numeric)::text,
 'optional_null', NULL::numeric,
 'numeric_zero', (0::numeric)::text,
 'json_exact_decimal', (jsonb_build_object('price', '1.2300'::text))->>'price',
 'json_u64', ('{"offset":18446744073709551615}'::jsonb->>'offset'),
 'negative_timestamp_violates_observed_check', NOT (-1::numeric >= 0 AND -1::numeric < 18446744073709551616::numeric)
);
COMMIT;
