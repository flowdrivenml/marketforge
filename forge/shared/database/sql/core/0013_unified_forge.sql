-- One source-object authority. Legacy remote names remain updatable compatibility
-- views; external names are read-only projections of immutable revision evidence.
ALTER TABLE historical.external_sources RENAME TO raw_sources;
CREATE VIEW historical.external_sources AS SELECT * FROM historical.raw_sources;
ALTER TABLE historical.remote_archives RENAME TO raw_objects;
ALTER TABLE historical.raw_objects ADD COLUMN acquisition_method TEXT NOT NULL DEFAULT 'exchange_download' CHECK(acquisition_method IN ('exchange_download','local_copy'));
ALTER TABLE historical.raw_objects ADD COLUMN raw_source_id BIGINT REFERENCES historical.raw_sources(id);
ALTER TABLE historical.raw_objects ALTER COLUMN start_at DROP NOT NULL;
ALTER TABLE historical.raw_objects ALTER COLUMN end_at DROP NOT NULL;
ALTER TABLE historical.raw_objects ADD COLUMN venue_code TEXT;
UPDATE historical.raw_objects r SET venue_code=e.code FROM catalog.exchanges e WHERE e.id=r.exchange_id;
ALTER TABLE historical.raw_objects ALTER COLUMN exchange_id DROP NOT NULL;
ALTER TABLE historical.raw_objects ADD CONSTRAINT raw_object_venue CHECK(venue_code IS NOT NULL AND btrim(venue_code)<>'');
ALTER TABLE historical.raw_objects ADD CONSTRAINT raw_object_coverage CHECK((start_at IS NULL)=(end_at IS NULL));
CREATE UNIQUE INDEX raw_object_source_key ON historical.raw_objects(raw_source_id,source_object_key) WHERE acquisition_method='local_copy';
CREATE VIEW historical.remote_archives AS SELECT id,exchange_id,scope_key,scope_json,data_type,source_object_key,filename,start_at,end_at,download_url,url_expires_at,source_etag,source_last_modified,remote_status,first_seen_at,last_seen_at,last_checked_at,source_metadata FROM historical.raw_objects WHERE acquisition_method='exchange_download';
-- Existing acquisition INSERTs use ON CONFLICT, which cannot target a view.
-- Application acquisition writes therefore use raw_objects directly.
ALTER TABLE historical.external_archives RENAME TO raw_interpretations;
ALTER TABLE historical.raw_interpretations ADD COLUMN raw_object_id BIGINT REFERENCES historical.raw_objects(id);
INSERT INTO historical.raw_objects(acquisition_method,raw_source_id,venue_code,exchange_id,scope_key,scope_json,data_type,source_object_key,filename,start_at,end_at,remote_status)
SELECT DISTINCT ON (a.external_source_id,a.source_object_key) 'local_copy',a.external_source_id,a.identity_json->>'exchange',e.id,
 'local:'||a.external_source_id||':'||a.source_object_key,
 jsonb_build_object('instrument_type',a.identity_json->>'instrument_type','market_category',a.identity_json->>'market_category','scope_kind','instrument','scope_identifier',a.identity_json->>'symbol'),
 a.data_type,a.source_object_key,a.source_object_key,a.declared_start_at,a.declared_end_at,'unknown'
FROM historical.raw_interpretations a LEFT JOIN catalog.exchanges e ON e.code=a.identity_json->>'exchange'
ORDER BY a.external_source_id,a.source_object_key,a.id;
UPDATE historical.raw_interpretations a SET raw_object_id=r.id FROM historical.raw_objects r
WHERE r.raw_source_id=a.external_source_id AND r.source_object_key=a.source_object_key;
ALTER TABLE historical.raw_interpretations ALTER COLUMN raw_object_id SET NOT NULL;
CREATE VIEW historical.external_archives AS SELECT id,external_source_id,source_object_key,sha256,bytes_expected,instrument_id,identity_json,specifications_json,data_type,declared_start_at,declared_end_at,format_profile,normalization_readiness,import_provenance,created_at FROM historical.raw_interpretations;
-- Explicit collision mapping: remote IDs retain their values. Local legacy IDs
-- retain revision identity and map to a newly allocated stable source object.
CREATE VIEW historical.legacy_raw_identity_map AS
 SELECT 'remote'::text AS legacy_origin,id AS legacy_id,id AS raw_object_id FROM historical.raw_objects WHERE acquisition_method='exchange_download'
 UNION ALL SELECT 'external',id,raw_object_id FROM historical.raw_interpretations;
ALTER TABLE historical.local_archives ADD COLUMN raw_object_id BIGINT REFERENCES historical.raw_objects(id);
UPDATE historical.local_archives l SET raw_object_id=COALESCE(l.remote_archive_id,a.raw_object_id)
FROM (SELECT NULL::BIGINT AS id,NULL::BIGINT AS raw_object_id UNION ALL SELECT id,raw_object_id FROM historical.raw_interpretations) a
WHERE l.external_archive_id IS NOT DISTINCT FROM a.id;
ALTER TABLE historical.local_archives ALTER COLUMN raw_object_id SET NOT NULL;
CREATE FUNCTION historical.bind_raw_object() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.raw_object_id := COALESCE(NEW.remote_archive_id,(SELECT raw_object_id FROM historical.raw_interpretations WHERE id=NEW.external_archive_id));
 IF NEW.raw_object_id IS NULL THEN RAISE EXCEPTION 'Missing unified raw object'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER local_raw_object BEFORE INSERT OR UPDATE OF remote_archive_id,external_archive_id ON historical.local_archives FOR EACH ROW EXECUTE FUNCTION historical.bind_raw_object();
CREATE FUNCTION historical.raw_venue() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.venue_code IS NULL THEN SELECT code INTO NEW.venue_code FROM catalog.exchanges WHERE id=NEW.exchange_id; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER raw_venue BEFORE INSERT ON historical.raw_objects FOR EACH ROW EXECUTE FUNCTION historical.raw_venue();
CREATE TABLE historical.instrument_authority (
 instrument_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 namespace TEXT NOT NULL,
 identity_key TEXT NOT NULL,
 definition JSONB NOT NULL,
 UNIQUE(namespace,identity_key), UNIQUE(namespace,instrument_id)
);
CREATE TABLE historical.specification_evidence (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 namespace TEXT NOT NULL,
 instrument_id BIGINT NOT NULL,
 fingerprint TEXT NOT NULL,
 definition JSONB NOT NULL,
 UNIQUE(namespace,instrument_id,fingerprint),
 FOREIGN KEY(namespace,instrument_id) REFERENCES historical.instrument_authority(namespace,instrument_id)
);
CREATE TABLE historical.registrations (
 id TEXT PRIMARY KEY,
 environment TEXT NOT NULL CHECK(environment='testing'),
 recipe_sha256 TEXT NOT NULL,
 evidence JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX datasets_engine_identity ON historical.datasets ((operation_details->>'instrument_namespace'),(operation_details->>'engine_dataset_id')) WHERE operation_details ? 'engine_dataset_id';
ALTER TABLE historical.raw_sources ADD COLUMN provider TEXT;
ALTER TABLE historical.raw_sources ADD COLUMN product TEXT;
ALTER TABLE historical.raw_sources ADD COLUMN provenance JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE historical.raw_interpretations ADD COLUMN binding_fingerprint TEXT NOT NULL DEFAULT 'legacy';
DO $$
DECLARE key_name TEXT;
BEGIN
 SELECT conname INTO STRICT key_name FROM pg_constraint WHERE conrelid='historical.raw_interpretations'::regclass AND contype='u' AND pg_get_constraintdef(oid)='UNIQUE (external_source_id, source_object_key, sha256)';
 EXECUTE format('ALTER TABLE historical.raw_interpretations DROP CONSTRAINT %I',key_name);
END $$;
ALTER TABLE historical.raw_interpretations ADD UNIQUE(external_source_id,source_object_key,sha256,binding_fingerprint);
ALTER TABLE historical.raw_objects DROP CONSTRAINT remote_archives_identity_key;
ALTER TABLE historical.raw_objects ADD CONSTRAINT remote_archives_identity_key UNIQUE NULLS NOT DISTINCT(exchange_id,scope_key,data_type,source_object_key);
INSERT INTO historical.instrument_authority(namespace,identity_key,definition)
SELECT 'marketforge:testing:historical:v1',e.code||'/'||i.instrument_type||'/'||i.market_category||'/'||i.symbol,
 jsonb_build_object('exchange',e.code,'instrument_type',i.instrument_type,'market_category',i.market_category,'symbol',i.symbol)
FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id ORDER BY i.id;
ALTER TABLE historical.datasets DROP CONSTRAINT datasets_check1;
ALTER TABLE historical.datasets ADD CONSTRAINT datasets_bounds CHECK(end_timestamp_ns>=start_timestamp_ns);
CREATE TABLE historical.raw_bindings (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 local_archive_id BIGINT NOT NULL REFERENCES historical.local_archives(id),
 interpretation_id BIGINT NOT NULL REFERENCES historical.raw_interpretations(id),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(local_archive_id,interpretation_id)
);
INSERT INTO historical.raw_bindings(local_archive_id,interpretation_id) SELECT id,external_archive_id FROM historical.local_archives WHERE external_archive_id IS NOT NULL;
-- New catalogue instruments receive an explicit persisted namespace mapping,
-- independent of file order and of their catalogue surrogate IDs.
CREATE FUNCTION historical.map_instrument_authority() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE venue TEXT;
BEGIN
 SELECT code INTO venue FROM catalog.exchanges WHERE id=NEW.exchange_id;
 INSERT INTO historical.instrument_authority(namespace,identity_key,definition)
 VALUES('marketforge:testing:historical:v1',venue||'/'||NEW.instrument_type||'/'||NEW.market_category||'/'||NEW.symbol,
 jsonb_build_object('exchange',venue,'instrument_type',NEW.instrument_type,'market_category',NEW.market_category,'symbol',NEW.symbol))
 ON CONFLICT(namespace,identity_key) DO NOTHING;
 RETURN NEW;
END $$;
CREATE TRIGGER instrument_canonical_mapping AFTER INSERT ON catalog.instruments FOR EACH ROW EXECUTE FUNCTION historical.map_instrument_authority();

-- Direct venue downloads also have source provenance, separate from venue and
-- object identity. URNs are stable source identifiers, never fabricated URLs.
INSERT INTO historical.raw_sources(source_key,source_kind,root_uri,provider,product,provenance)
SELECT 'marketforge.exchange.'||code,'other','urn:marketforge:exchange:'||code,code,'historical_bulk',
 jsonb_build_object('acquisition_method','exchange_download','provider_relationship','direct_venue')
FROM catalog.exchanges;
UPDATE historical.raw_objects o SET raw_source_id=s.id FROM historical.raw_sources s
WHERE o.acquisition_method='exchange_download' AND s.source_key='marketforge.exchange.'||o.venue_code;
CREATE OR REPLACE FUNCTION historical.raw_venue() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE venue_source BIGINT;
BEGIN
 IF NEW.venue_code IS NULL THEN SELECT code INTO NEW.venue_code FROM catalog.exchanges WHERE id=NEW.exchange_id; END IF;
 IF NEW.acquisition_method='exchange_download' THEN
   INSERT INTO historical.raw_sources(source_key,source_kind,root_uri,provider,product,provenance)
   VALUES('marketforge.exchange.'||NEW.venue_code,'other','urn:marketforge:exchange:'||NEW.venue_code,NEW.venue_code,'historical_bulk',
   jsonb_build_object('acquisition_method','exchange_download','provider_relationship','direct_venue'))
   ON CONFLICT(source_key) DO UPDATE SET source_key=EXCLUDED.source_key
   WHERE raw_sources.root_uri=EXCLUDED.root_uri AND raw_sources.provider=EXCLUDED.provider
   RETURNING id INTO venue_source;
   IF venue_source IS NULL THEN RAISE EXCEPTION 'Conflicting exchange source provenance'; END IF;
   NEW.raw_source_id := venue_source;
 END IF;
 RETURN NEW;
END $$;
