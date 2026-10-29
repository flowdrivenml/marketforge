-- ============================================================
-- Persistent streaming instance definitions
-- ============================================================

CREATE TABLE live.stream_definitions (
    stream_id TEXT PRIMARY KEY,

    -- 1 = source
    -- 2 = derived
    origin_kind SMALLINT NOT NULL,

    -- Existing live.streams identity for source streams.
    -- Derived streams do not require an instrument-backed
    -- recording stream during their initial implementation.
    recording_stream_key BIGINT
        REFERENCES live.streams(id)
        ON DELETE RESTRICT,

    -- Typed Rust creation request, serialized as JSON.
    definition JSONB NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT true,

    -- 1 = on_demand
    -- 2 = always_on
    activation_policy SMALLINT NOT NULL DEFAULT 1,

    revision BIGINT NOT NULL DEFAULT 1,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT stream_definition_id_nonempty
        CHECK (
            length(btrim(stream_id)) > 0
            AND stream_id = btrim(stream_id)
        ),

    CONSTRAINT stream_definition_origin_valid
        CHECK (origin_kind IN (1, 2)),

    CONSTRAINT stream_definition_recording_binding_valid
        CHECK (
            (origin_kind = 1 AND recording_stream_key IS NOT NULL)
            OR
            (origin_kind = 2 AND recording_stream_key IS NULL)
        ),

    CONSTRAINT stream_definition_payload_object
        CHECK (jsonb_typeof(definition) = 'object'),

    CONSTRAINT stream_definition_activation_valid
        CHECK (activation_policy IN (1, 2)),

    CONSTRAINT stream_definition_revision_valid
        CHECK (revision > 0),

    CONSTRAINT stream_definition_source_id_consistent
        CHECK (
            origin_kind <> 1
            OR (
                NOT definition ? 'stream_id'
                AND definition ? 'instrument_id'
                AND definition ? 'channel'
            )
        ),

    CONSTRAINT stream_definition_derived_name_consistent
        CHECK (
            origin_kind <> 2
            OR definition->>'name' = stream_id
        ),

    CONSTRAINT stream_definition_recording_key_unique
        UNIQUE (recording_stream_key)
);

COMMENT ON TABLE live.stream_definitions IS
    'Persistent desired definitions for MarketForge source and derived streaming instances.';

COMMENT ON COLUMN live.stream_definitions.enabled IS
    'Persistent permission to run; not actual runtime state.';

COMMENT ON COLUMN live.stream_definitions.activation_policy IS
    '1 = on_demand, 2 = always_on';

-- ============================================================
-- Durable derived-stream dependencies
-- ============================================================

CREATE TABLE live.stream_dependencies (
    derived_stream_id TEXT NOT NULL
        REFERENCES live.stream_definitions(stream_id)
        ON DELETE RESTRICT,

    input_stream_id TEXT NOT NULL
        REFERENCES live.stream_definitions(stream_id)
        ON DELETE RESTRICT,

    input_position INTEGER NOT NULL,

    CONSTRAINT stream_dependencies_pkey
        PRIMARY KEY (derived_stream_id, input_stream_id),

    CONSTRAINT stream_dependencies_position_unique
        UNIQUE (derived_stream_id, input_position),

    CONSTRAINT stream_dependencies_position_nonnegative
        CHECK (input_position >= 0),

    CONSTRAINT stream_dependencies_no_self_reference
        CHECK (derived_stream_id <> input_stream_id)
);

CREATE INDEX stream_dependencies_input_idx
    ON live.stream_dependencies(input_stream_id);

COMMENT ON TABLE live.stream_dependencies IS
    'Persistent dependency edges from derived streams to their required input streams.';
