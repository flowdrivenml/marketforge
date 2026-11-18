-- Explicit declarations remain stronger than projected catalogue observations.
ALTER TABLE historical.specification_evidence
 ADD COLUMN evidence_kind TEXT NOT NULL DEFAULT 'historical_declaration'
 CHECK(evidence_kind IN ('historically_verified','historical_declaration','catalogue_inferred','reference_contract_inferred')),
 ADD COLUMN policy_version TEXT,
 ADD COLUMN source_instrument_id BIGINT REFERENCES catalog.instruments(id),
 ADD COLUMN source_observation_id BIGINT REFERENCES catalog.instrument_api_raw(id),
 ADD COLUMN source_observed_at TIMESTAMPTZ,
 ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX specification_evidence_resolution ON historical.specification_evidence(namespace,instrument_id,evidence_kind,id DESC);
CREATE TABLE historical.family_authority (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 namespace TEXT NOT NULL,
 exchange TEXT NOT NULL,
 instrument_type TEXT NOT NULL CHECK(instrument_type IN ('perpetual','future','option')),
 market_category TEXT NOT NULL,
 identifier TEXT NOT NULL,
 UNIQUE(namespace,exchange,instrument_type,market_category,identifier)
);
CREATE TABLE historical.family_membership_evidence (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 family_id BIGINT NOT NULL REFERENCES historical.family_authority(id),
 namespace TEXT NOT NULL,
 instrument_id BIGINT NOT NULL,
 method TEXT NOT NULL CHECK(method IN ('observed','catalogue_inferred','archive_derived')),
 fingerprint TEXT NOT NULL CHECK(fingerprint ~ '^[a-f0-9]{64}$'),
 definition JSONB NOT NULL CHECK(jsonb_typeof(definition)='object'),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 FOREIGN KEY(namespace,instrument_id) REFERENCES historical.instrument_authority(namespace,instrument_id),
 UNIQUE(family_id,instrument_id,fingerprint)
);
CREATE INDEX family_membership_lookup ON historical.family_membership_evidence(family_id,instrument_id,id DESC);
CREATE TABLE historical.archive_identity_evidence (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 local_archive_id BIGINT NOT NULL REFERENCES historical.local_archives(id),
 input_sha256 TEXT NOT NULL,
 profile_fingerprint TEXT NOT NULL,
 policy_version TEXT NOT NULL,
 definition JSONB NOT NULL CHECK(jsonb_typeof(definition)='object'),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(local_archive_id,input_sha256,profile_fingerprint,policy_version)
);
CREATE TABLE historical.metadata_enrichment_runs (
 id TEXT PRIMARY KEY,
 environment TEXT NOT NULL CHECK(environment='testing'),
 policy_version TEXT NOT NULL,
 report JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE FUNCTION historical.immutable_metadata_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION 'Historical metadata evidence is immutable; append a revision';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER specification_evidence_immutable BEFORE UPDATE OR DELETE ON historical.specification_evidence FOR EACH ROW EXECUTE FUNCTION historical.immutable_metadata_evidence();
CREATE TRIGGER family_membership_immutable BEFORE UPDATE OR DELETE ON historical.family_membership_evidence FOR EACH ROW EXECUTE FUNCTION historical.immutable_metadata_evidence();
CREATE TRIGGER archive_identity_immutable BEFORE UPDATE OR DELETE ON historical.archive_identity_evidence FOR EACH ROW EXECUTE FUNCTION historical.immutable_metadata_evidence();
