use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::live::stream::request::{
    CreateDerivedStreamRequest, CreateSourceStreamRequest, SourceChannel,
};

use super::model::{
    ActivationPolicy, DesiredStreamState, PersistedStreamDefinition, StreamDefinition,
    StreamDependency, StreamOrigin,
};

#[derive(Debug, Error)]
pub enum StreamDefinitionError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    #[error("invalid stream definition: {0}")]
    Invalid(String),

    #[error("stream already exists: {0}")]
    AlreadyExists(String),

    #[error("stream not found: {0}")]
    NotFound(String),

    #[error("stream revision conflict: {0}")]
    RevisionConflict(String),

    #[error("source registration mismatch: {0}")]
    RegistrationMismatch(String),

    #[error("stream has dependent definitions: {0}")]
    HasDependents(String),
}

#[derive(Clone)]
pub struct StreamDefinitionRepository {
    pool: PgPool,
}

impl StreamDefinitionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Atomically accept a source registration and its durable desired definition.
    /// A duplicate or failed definition cannot leave an orphan registration.
    pub async fn register_source(
        &self,
        stream_id: &str,
        request: CreateSourceStreamRequest,
        desired: DesiredStreamState,
    ) -> Result<PersistedStreamDefinition, StreamDefinitionError> {
        validate_stream_id(stream_id)?;
        request.validate().map_err(StreamDefinitionError::Invalid)?;
        let kind = match request.channel { SourceChannel::Trades => 1_i16, SourceChannel::Depth => 2_i16 };
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO live.streams (stream_id, instrument_id, stream_kind) VALUES ($1,$2,$3) ON CONFLICT (stream_id) DO NOTHING")
            .bind(stream_id).bind(request.instrument_id).bind(kind).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT id, instrument_id, stream_kind FROM live.streams WHERE stream_id=$1 FOR KEY SHARE")
            .bind(stream_id).fetch_one(&mut *tx).await?;
        if row.try_get::<i64,_>("instrument_id")? != request.instrument_id || row.try_get::<i16,_>("stream_kind")? != kind {
            return Err(StreamDefinitionError::RegistrationMismatch(stream_id.into()));
        }
        let key: i64 = row.try_get("id")?;
        let definition = StreamDefinition::Source(request);
        insert_definition(&mut tx, stream_id, Some(key), &definition, desired).await?;
        let persisted = PersistedStreamDefinition {
            stream_id: stream_id.into(), recording_stream_key: Some(key), definition, desired, revision: 1,
        };
        tx.commit().await?;
        Ok(persisted)
    }

    /// Persist a source definition after its existing
    /// live.streams registration has been established.
    pub async fn create_source(
        &self,
        stream_id: &str,
        recording_stream_key: i64,
        request: CreateSourceStreamRequest,
        desired: DesiredStreamState,
    ) -> Result<PersistedStreamDefinition, StreamDefinitionError> {
        validate_stream_id(stream_id)?;

        request.validate().map_err(StreamDefinitionError::Invalid)?;

        if recording_stream_key <= 0 {
            return Err(StreamDefinitionError::Invalid(
                "recording_stream_key must be positive".into(),
            ));
        }

        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            SELECT
                stream_id,
                instrument_id,
                stream_kind
            FROM live.streams
            WHERE id = $1
            FOR KEY SHARE
            "#,
        )
        .bind(recording_stream_key)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            return Err(StreamDefinitionError::RegistrationMismatch(
                stream_id.to_owned(),
            ));
        };

        let expected_kind = match request.channel {
            SourceChannel::Trades => 1_i16,
            SourceChannel::Depth => 2_i16,
        };

        let matches_registration = row.try_get::<String, _>("stream_id")? == stream_id
            && row.try_get::<i64, _>("instrument_id")? == request.instrument_id
            && row.try_get::<i16, _>("stream_kind")? == expected_kind;

        if !matches_registration {
            return Err(StreamDefinitionError::RegistrationMismatch(
                stream_id.to_owned(),
            ));
        }

        let definition = StreamDefinition::Source(request);

        insert_definition(
            &mut tx,
            stream_id,
            Some(recording_stream_key),
            &definition,
            desired,
        )
        .await?;

        tx.commit().await?;

        self.find(stream_id)
            .await?
            .ok_or_else(|| StreamDefinitionError::NotFound(stream_id.into()))
    }

    /// Persist a derived definition and all its dependency
    /// edges in the same PostgreSQL transaction.
    pub async fn create_derived(
        &self,
        request: CreateDerivedStreamRequest,
        desired: DesiredStreamState,
    ) -> Result<PersistedStreamDefinition, StreamDefinitionError> {
        request.validate().map_err(StreamDefinitionError::Invalid)?;

        let stream_id = request.name.clone();
        let inputs = request.inputs.clone();

        validate_stream_id(&stream_id)?;

        let mut tx = self.pool.begin().await?;

        // Lock existing inputs so that they cannot disappear
        // while this definition and its edges are created.
        //
        // Input compatibility is deliberately NOT assumed here.
        // The registry will validate that separately.
        for input in &inputs {
            let exists = sqlx::query_scalar::<_, String>(
                r#"
                SELECT stream_id
                FROM live.stream_definitions
                WHERE stream_id = $1
                FOR KEY SHARE
                "#,
            )
            .bind(input)
            .fetch_optional(&mut *tx)
            .await?;

            if exists.is_none() {
                return Err(StreamDefinitionError::NotFound(input.clone()));
            }
        }

        let definition = StreamDefinition::Derived(request);

        insert_definition(&mut tx, &stream_id, None, &definition, desired).await?;

        for (index, input) in inputs.iter().enumerate() {
            let position = i32::try_from(index).map_err(|_| {
                StreamDefinitionError::Invalid("too many derived-stream inputs".into())
            })?;

            sqlx::query(
                r#"
                INSERT INTO live.stream_dependencies (
                    derived_stream_id,
                    input_stream_id,
                    input_position
                )
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(&stream_id)
            .bind(input)
            .bind(position)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        self.find(&stream_id)
            .await?
            .ok_or(StreamDefinitionError::NotFound(stream_id))
    }

    pub async fn find(
        &self,
        stream_id: &str,
    ) -> Result<Option<PersistedStreamDefinition>, StreamDefinitionError> {
        let row = sqlx::query(
            r#"
            SELECT
                stream_id,
                origin_kind,
                recording_stream_key,
                definition,
                enabled,
                activation_policy,
                revision
            FROM live.stream_definitions
            WHERE stream_id = $1
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(decode_definition).transpose()
    }

    /// Load persistent definitions for runtime restoration.
    ///
    /// This does not start any instances.
    pub async fn list(&self) -> Result<Vec<PersistedStreamDefinition>, StreamDefinitionError> {
        let rows = sqlx::query(
            r#"
            SELECT
                stream_id,
                origin_kind,
                recording_stream_key,
                definition,
                enabled,
                activation_policy,
                revision
            FROM live.stream_definitions
            ORDER BY stream_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(decode_definition).collect()
    }

    /// Load the durable dependency graph in stable order.
    pub async fn dependencies(&self) -> Result<Vec<StreamDependency>, StreamDefinitionError> {
        let rows = sqlx::query(
            r#"
            SELECT
                derived_stream_id,
                input_stream_id,
                input_position
            FROM live.stream_dependencies
            ORDER BY derived_stream_id, input_position
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(StreamDependency {
                    derived_stream_id: row.try_get("derived_stream_id")?,
                    input_stream_id: row.try_get("input_stream_id")?,
                    input_position: row.try_get("input_position")?,
                })
            })
            .collect()
    }

    /// Update persistent desired state using optimistic
    /// concurrency control.
    ///
    /// The runtime registry MUST authorize changes before
    /// calling this method.
    ///
    /// In particular, this repository does not decide whether
    /// disabling a stream would interrupt active dependents.
    pub async fn update_desired_state(
        &self,
        stream_id: &str,
        expected_revision: i64,
        desired: DesiredStreamState,
    ) -> Result<PersistedStreamDefinition, StreamDefinitionError> {
        if expected_revision <= 0 {
            return Err(StreamDefinitionError::Invalid(
                "expected_revision must be positive".into(),
            ));
        }

        let row = sqlx::query(
            r#"
            UPDATE live.stream_definitions
            SET
                enabled = $3,
                activation_policy = $4,
                revision = revision + 1,
                updated_at = now()
            WHERE stream_id = $1
              AND revision = $2
              AND revision < 9223372036854775807
            RETURNING
                stream_id,
                origin_kind,
                recording_stream_key,
                definition,
                enabled,
                activation_policy,
                revision
            "#,
        )
        .bind(stream_id)
        .bind(expected_revision)
        .bind(desired.enabled)
        .bind(desired.activation_policy.as_db())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => decode_definition(row),

            None => {
                if self.find(stream_id).await?.is_none() {
                    Err(StreamDefinitionError::NotFound(stream_id.to_owned()))
                } else {
                    Err(StreamDefinitionError::RevisionConflict(
                        stream_id.to_owned(),
                    ))
                }
            }
        }
    }

    /// Permanently delete a definition without deleting
    /// historical trades, L2 records, or live.streams.
    ///
    /// The caller must first ensure that the instance has
    /// been stopped and all ephemeral demand removed.
    pub async fn delete(&self, stream_id: &str) -> Result<(), StreamDefinitionError> {
        let mut tx = self.pool.begin().await?;

        let exists = sqlx::query_scalar::<_, String>(
            r#"
            SELECT stream_id
            FROM live.stream_definitions
            WHERE stream_id = $1
            FOR UPDATE
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            return Err(StreamDefinitionError::NotFound(stream_id.to_owned()));
        }

        // Delete only edges belonging to the definition.
        // Other definitions depending on this stream are
        // still protected by foreign keys.
        sqlx::query(
            r#"
            DELETE FROM live.stream_dependencies
            WHERE derived_stream_id = $1
            "#,
        )
        .bind(stream_id)
        .execute(&mut *tx)
        .await?;

        let result = sqlx::query(
            r#"
            DELETE FROM live.stream_definitions
            WHERE stream_id = $1
            "#,
        )
        .bind(stream_id)
        .execute(&mut *tx)
        .await;

        match result {
            Ok(_) => {
                tx.commit().await?;
                Ok(())
            }

            Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23503") => {
                Err(StreamDefinitionError::HasDependents(stream_id.to_owned()))
            }

            Err(error) => Err(error.into()),
        }
    }
}

async fn insert_definition(
    tx: &mut Transaction<'_, Postgres>,
    stream_id: &str,
    recording_stream_key: Option<i64>,
    definition: &StreamDefinition,
    desired: DesiredStreamState,
) -> Result<(), StreamDefinitionError> {
    definition
        .validate()
        .map_err(StreamDefinitionError::Invalid)?;

    let payload = match definition {
        StreamDefinition::Source(request) => serde_json::to_value(request)?,

        StreamDefinition::Derived(request) => serde_json::to_value(request)?,
    };

    let result = sqlx::query(
        r#"
        INSERT INTO live.stream_definitions (
            stream_id,
            origin_kind,
            recording_stream_key,
            definition,
            enabled,
            activation_policy
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (stream_id) DO NOTHING
        "#,
    )
    .bind(stream_id)
    .bind(definition.origin().as_db())
    .bind(recording_stream_key)
    .bind(sqlx::types::Json(payload))
    .bind(desired.enabled)
    .bind(desired.activation_policy.as_db())
    .execute(&mut **tx)
    .await?;

    if result.rows_affected() != 1 {
        return Err(StreamDefinitionError::AlreadyExists(stream_id.to_owned()));
    }

    Ok(())
}

fn decode_definition(
    row: sqlx::postgres::PgRow,
) -> Result<PersistedStreamDefinition, StreamDefinitionError> {
    let stream_id: String = row.try_get("stream_id")?;
    let origin_value: i16 = row.try_get("origin_kind")?;
    let activation_value: i16 = row.try_get("activation_policy")?;

    let origin = StreamOrigin::from_db(origin_value).map_err(StreamDefinitionError::Invalid)?;

    let activation_policy =
        ActivationPolicy::from_db(activation_value).map_err(StreamDefinitionError::Invalid)?;

    let sqlx::types::Json(payload): sqlx::types::Json<serde_json::Value> =
        row.try_get("definition")?;

    let definition = match origin {
        StreamOrigin::Source => StreamDefinition::Source(serde_json::from_value(payload)?),

        StreamOrigin::Derived => StreamDefinition::Derived(serde_json::from_value(payload)?),
    };

    definition
        .validate()
        .map_err(StreamDefinitionError::Invalid)?;

    let recording_stream_key: Option<i64> = row.try_get("recording_stream_key")?;

    match &definition {
        StreamDefinition::Source(_) if recording_stream_key.is_none() => {
            return Err(StreamDefinitionError::Invalid(
                "source definition has no stream registration".into(),
            ));
        }

        StreamDefinition::Derived(request) => {
            if request.name != stream_id || recording_stream_key.is_some() {
                return Err(StreamDefinitionError::Invalid(
                    "derived definition identity is inconsistent".into(),
                ));
            }
        }

        _ => {}
    }

    Ok(PersistedStreamDefinition {
        stream_id,
        recording_stream_key,
        definition,
        desired: DesiredStreamState {
            enabled: row.try_get("enabled")?,
            activation_policy,
        },
        revision: row.try_get("revision")?,
    })
}

fn validate_stream_id(stream_id: &str) -> Result<(), StreamDefinitionError> {
    if stream_id.is_empty()
        || stream_id.trim() != stream_id
        || stream_id.chars().any(char::is_control)
    {
        return Err(StreamDefinitionError::Invalid("invalid stream_id".into()));
    }

    Ok(())
}
