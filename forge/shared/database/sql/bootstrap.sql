-- Framework metadata only; core migrations remain separate.
CREATE SCHEMA IF NOT EXISTS forge_meta;

CREATE TABLE IF NOT EXISTS forge_meta.database_identity (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    database_id UUID NOT NULL DEFAULT gen_random_uuid(),
    purpose TEXT NOT NULL CHECK (purpose IN ('production', 'testing'))
);

CREATE TABLE IF NOT EXISTS forge_meta.schema_migrations (
    stream TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    name TEXT NOT NULL,
    checksum TEXT NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream, version),
    UNIQUE (stream, name)
);
