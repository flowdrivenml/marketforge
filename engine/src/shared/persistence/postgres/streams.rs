use sqlx::{PgPool, Row};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum StreamKind {
    Trade = 1,
    L2 = 2,
}

impl StreamKind {
    pub fn from_db(value: i16) -> Result<Self, StreamRepositoryError> {
        match value {
            1 => Ok(Self::Trade),
            2 => Ok(Self::L2),
            _ => Err(StreamRepositoryError::UnknownStreamKind(value)),
        }
    }

    pub fn as_db(self) -> i16 {
        self as i16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredStream {
    pub id: i64,
    pub stream_id: String,
    pub instrument_id: i64,
    pub kind: StreamKind,
}

#[derive(Debug, Clone)]
pub struct RegisterStreamRequest {
    pub stream_id: String,
    pub instrument_id: i64,
    pub kind: StreamKind,
}

#[derive(Debug)]
pub enum StreamRepositoryError {
    Database(sqlx::Error),
    InvalidStreamId,
    UnknownStreamKind(i16),
    ConflictingRegistration(String),
}

impl fmt::Display for StreamRepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(f, "database error: {error}"),
            Self::InvalidStreamId => write!(f, "stream_id cannot be empty"),
            Self::UnknownStreamKind(value) => {
                write!(f, "unknown stream kind: {value}")
            }
            Self::ConflictingRegistration(stream_id) => {
                write!(
                    f,
                    "stream {stream_id} is already registered with different metadata"
                )
            }
        }
    }
}

impl std::error::Error for StreamRepositoryError {}

impl From<sqlx::Error> for StreamRepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

pub struct StreamRepository {
    pool: PgPool,
}

impl StreamRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn register(
        &self,
        request: RegisterStreamRequest,
    ) -> Result<RegisteredStream, StreamRepositoryError> {
        if request.stream_id.trim().is_empty() {
            return Err(StreamRepositoryError::InvalidStreamId);
        }

        let row = sqlx::query(
            r#"
            INSERT INTO live.streams (
                stream_id,
                instrument_id,
                stream_kind
            )
            VALUES ($1, $2, $3)

            ON CONFLICT (stream_id)
            DO UPDATE SET
                stream_id = EXCLUDED.stream_id
            WHERE
                live.streams.instrument_id = EXCLUDED.instrument_id
                AND live.streams.stream_kind = EXCLUDED.stream_kind

            RETURNING
                id,
                stream_id,
                instrument_id,
                stream_kind
            "#,
        )
        .bind(&request.stream_id)
        .bind(request.instrument_id)
        .bind(request.kind.as_db())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Self::decode(row),
            None => Err(StreamRepositoryError::ConflictingRegistration(
                request.stream_id,
            )),
        }
    }

    pub async fn find_by_stream_id(
        &self,
        stream_id: &str,
    ) -> Result<Option<RegisteredStream>, StreamRepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                stream_id,
                instrument_id,
                stream_kind
            FROM live.streams
            WHERE stream_id = $1
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(Self::decode).transpose()
    }

    pub async fn find_by_id(
        &self,
        id: i64,
    ) -> Result<Option<RegisteredStream>, StreamRepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                stream_id,
                instrument_id,
                stream_kind
            FROM live.streams
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(Self::decode).transpose()
    }

    fn decode(row: sqlx::postgres::PgRow) -> Result<RegisteredStream, StreamRepositoryError> {
        let kind = StreamKind::from_db(row.try_get("stream_kind")?)?;

        Ok(RegisteredStream {
            id: row.try_get("id")?,
            stream_id: row.try_get("stream_id")?,
            instrument_id: row.try_get("instrument_id")?,
            kind,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_kind_roundtrip() {
        assert_eq!(
            StreamKind::from_db(StreamKind::Trade.as_db()).unwrap(),
            StreamKind::Trade
        );

        assert_eq!(
            StreamKind::from_db(StreamKind::L2.as_db()).unwrap(),
            StreamKind::L2
        );
    }

    #[test]
    fn rejects_unknown_stream_kind() {
        assert!(matches!(
            StreamKind::from_db(999),
            Err(StreamRepositoryError::UnknownStreamKind(999))
        ));
    }

    #[test]
    fn trade_kind_matches_database_contract() {
        assert_eq!(StreamKind::Trade.as_db(), 1);
    }

    #[test]
    fn l2_kind_matches_database_contract() {
        assert_eq!(StreamKind::L2.as_db(), 2);
    }
}
