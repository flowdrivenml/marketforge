use sqlx::{PgPool, Postgres, Transaction};

use crate::shared::domain::canonical::{BookSide, CanonicalEvent, TradeSide};

/// A canonical event with an externally assigned recording identity.
///
/// The stream must already be registered in live.streams.
/// Offsets must be stable across retries.
#[derive(Debug, Clone)]
pub struct RecordingEvent {
    pub stream_key: i64,
    pub log_epoch: u64,
    pub event_offset: u64,
    pub event: CanonicalEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchWriteResult {
    pub records_written: usize,
}

#[derive(Debug, Clone)]
pub struct PostgresBatchWriter {
    pool: PgPool,
}

impl PostgresBatchWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Atomically persists a batch.
    ///
    /// All rows are committed, or none are.
    pub async fn write_batch(
        &self,
        batch: &[RecordingEvent],
    ) -> Result<BatchWriteResult, sqlx::Error> {
        if batch.is_empty() {
            return Ok(BatchWriteResult { records_written: 0 });
        }

        let mut transaction = self.pool.begin().await?;

        for record in batch {
            match &record.event {
                CanonicalEvent::Trade(trade) => {
                    insert_trade(&mut transaction, record, trade).await?;
                }

                CanonicalEvent::L2LevelUpdate(level) => {
                    insert_l2(&mut transaction, record, level).await?;
                }
            }
        }

        transaction.commit().await?;

        Ok(BatchWriteResult {
            records_written: batch.len(),
        })
    }
}

async fn insert_trade(
    tx: &mut Transaction<'_, Postgres>,
    record: &RecordingEvent,
    trade: &crate::shared::domain::canonical::Trade,
) -> Result<(), sqlx::Error> {
    let side: i16 = match trade.side {
        TradeSide::Buy => 1,
        TradeSide::Sell => 2,
    };

    let epoch = record.log_epoch.to_string();
    let offset = record.event_offset.to_string();
    let sequence = trade.sequence.map(|value| value.to_string());

    sqlx::query(
        r#"
        INSERT INTO live.trades (
            stream_key,
            log_epoch,
            event_offset,
            event_timestamp_ns,
            system_timestamp_ns,
            trade_id,
            sequence,
            side,
            price,
            quantity_base,
            quantity_quote,
            quantity_contracts,
            is_rpi,
            trade_iv,
            mark_iv,
            index_price,
            mark_price
        )
        VALUES (
            $1,
            $2::numeric,
            $3::numeric,
            $4,
            $5,
            $6,
            $7::numeric,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16,
            $17
        )
        "#,
    )
    .bind(record.stream_key)
    .bind(epoch)
    .bind(offset)
    .bind(trade.envelope.event_timestamp_ns)
    .bind(trade.envelope.system_timestamp_ns)
    .bind(&trade.trade_id)
    .bind(sequence)
    .bind(side)
    .bind(trade.price)
    .bind(trade.quantity_base)
    .bind(trade.quantity_quote)
    .bind(trade.quantity_contracts)
    .bind(trade.is_rpi)
    .bind(trade.trade_iv)
    .bind(trade.mark_iv)
    .bind(trade.index_price)
    .bind(trade.mark_price)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn insert_l2(
    tx: &mut Transaction<'_, Postgres>,
    record: &RecordingEvent,
    level: &crate::shared::domain::canonical::L2LevelUpdate,
) -> Result<(), sqlx::Error> {
    let side: i16 = match level.side {
        BookSide::Bid => 1,
        BookSide::Ask => 2,
    };

    let epoch = record.log_epoch.to_string();
    let offset = record.event_offset.to_string();
    let order_count = level.order_count.map(|value| value.to_string());

    sqlx::query(
        r#"
        INSERT INTO live.l2_updates (
            stream_key,
            log_epoch,
            event_offset,
            event_timestamp_ns,
            system_timestamp_ns,
            side,
            price,
            quantity_base,
            quantity_quote,
            quantity_contracts,
            order_count
        )
        VALUES (
            $1,
            $2::numeric,
            $3::numeric,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11::numeric
        )
        "#,
    )
    .bind(record.stream_key)
    .bind(epoch)
    .bind(offset)
    .bind(level.envelope.event_timestamp_ns)
    .bind(level.envelope.system_timestamp_ns)
    .bind(side)
    .bind(level.price)
    .bind(level.quantity_base)
    .bind(level.quantity_quote)
    .bind(level.quantity_contracts)
    .bind(order_count)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    use rust_decimal::Decimal;
    use sqlx::{PgPool, Row};

    use crate::shared::domain::canonical::{
        BookSide, EventEnvelope, Exchange, L2LevelUpdate, Trade, TradeSide,
    };

    fn unique_id(prefix: &str) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();

        format!("{prefix}-{}-{timestamp}", std::process::id())
    }

    fn envelope(stream_id: &str, instrument_id: i64) -> EventEnvelope {
        EventEnvelope {
            event_timestamp_ns: 1_761_000_000_123_456_789,
            system_timestamp_ns: Some(1_761_000_000_123_456_999),
            exchange: Exchange::Bybit,
            instrument_id,
            symbol: "BTCUSDT".to_owned(),
            stream_id: stream_id.to_owned(),
        }
    }

    fn trade(stream_id: &str, instrument_id: i64) -> CanonicalEvent {
        CanonicalEvent::Trade(Trade {
            envelope: envelope(stream_id, instrument_id),

            trade_id: Some("123456789".to_owned()),
            sequence: Some(u64::MAX),

            side: TradeSide::Buy,
            price: Decimal::new(123456789, 4),

            quantity_base: Some(Decimal::new(123456789, 8)),
            quantity_quote: Some(Decimal::new(1524157875, 8)),
            quantity_contracts: None,

            is_rpi: Some(false),

            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        })
    }

    fn l2(stream_id: &str, instrument_id: i64, quantity: Decimal) -> CanonicalEvent {
        CanonicalEvent::L2LevelUpdate(L2LevelUpdate {
            envelope: envelope(stream_id, instrument_id),

            side: BookSide::Bid,
            price: Decimal::new(123456789, 4),

            quantity_base: Some(quantity),
            quantity_quote: None,
            quantity_contracts: None,

            order_count: Some(u64::MAX),
        })
    }

    async fn test_pool() -> PgPool {
        let url = std::env::var("MARKETFORGE_DATABASE_URL")
            .expect("MARKETFORGE_DATABASE_URL must be configured");

        PgPool::connect(&url)
            .await
            .expect("failed to connect to PostgreSQL")
    }

    async fn existing_instrument(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT id FROM catalog.instruments ORDER BY id LIMIT 1")
            .fetch_one(pool)
            .await
            .expect("test catalog must contain at least one instrument")
    }

    async fn register_test_stream(
        pool: &PgPool,
        stream_id: &str,
        instrument_id: i64,
        kind: i16,
    ) -> i64 {
        sqlx::query_scalar(
            r#"
            INSERT INTO live.streams (
                stream_id,
                instrument_id,
                stream_kind
            )
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(stream_id)
        .bind(instrument_id)
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn cleanup_stream(pool: &PgPool, stream_key: i64) {
        sqlx::query("DELETE FROM live.trades WHERE stream_key = $1")
            .bind(stream_key)
            .execute(pool)
            .await
            .unwrap();

        sqlx::query("DELETE FROM live.l2_updates WHERE stream_key = $1")
            .bind(stream_key)
            .execute(pool)
            .await
            .unwrap();

        sqlx::query("DELETE FROM live.streams WHERE id = $1")
            .bind(stream_key)
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL test database"]
    async fn postgres_trade_roundtrip_preserves_exact_values() {
        let pool = test_pool().await;
        let instrument_id = existing_instrument(&pool).await;

        let stream_id = unique_id("marketforge-trade-test");

        let stream_key = register_test_stream(&pool, &stream_id, instrument_id, 1).await;

        let writer = PostgresBatchWriter::new(pool.clone());

        let record = RecordingEvent {
            stream_key,
            log_epoch: u64::MAX,
            event_offset: u64::MAX,
            event: trade(&stream_id, instrument_id),
        };

        let result = writer.write_batch(&[record]).await.unwrap();

        assert_eq!(result.records_written, 1);

        let row = sqlx::query(
            r#"
            SELECT
                log_epoch::text AS epoch,
                event_offset::text AS offset_value,
                event_timestamp_ns,
                system_timestamp_ns,
                trade_id,
                sequence::text AS sequence_value,
                side,
                price,
                quantity_base,
                quantity_quote,
                quantity_contracts
            FROM live.trades
            WHERE stream_key = $1
            "#,
        )
        .bind(stream_key)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.get::<String, _>("epoch"), u64::MAX.to_string());

        assert_eq!(row.get::<String, _>("offset_value"), u64::MAX.to_string());

        assert_eq!(row.get::<String, _>("sequence_value"), u64::MAX.to_string());

        assert_eq!(
            row.get::<i64, _>("event_timestamp_ns"),
            1_761_000_000_123_456_789
        );

        assert_eq!(
            row.get::<Option<i64>, _>("system_timestamp_ns"),
            Some(1_761_000_000_123_456_999)
        );

        assert_eq!(row.get::<Decimal, _>("price"), Decimal::new(123456789, 4));

        assert_eq!(
            row.get::<Option<Decimal>, _>("quantity_base"),
            Some(Decimal::new(123456789, 8))
        );

        assert_eq!(
            row.get::<Option<Decimal>, _>("quantity_quote"),
            Some(Decimal::new(1524157875, 8))
        );

        assert_eq!(row.get::<Option<Decimal>, _>("quantity_contracts"), None);

        cleanup_stream(&pool, stream_key).await;
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL test database"]
    async fn postgres_l2_preserves_zero_and_null() {
        let pool = test_pool().await;
        let instrument_id = existing_instrument(&pool).await;

        let stream_id = unique_id("marketforge-l2-test");

        let stream_key = register_test_stream(&pool, &stream_id, instrument_id, 2).await;

        let writer = PostgresBatchWriter::new(pool.clone());

        let record = RecordingEvent {
            stream_key,
            log_epoch: 1,
            event_offset: 1,
            event: l2(&stream_id, instrument_id, Decimal::ZERO),
        };

        writer.write_batch(&[record]).await.unwrap();

        let row = sqlx::query(
            r#"
            SELECT
                quantity_base,
                quantity_quote,
                quantity_contracts,
                order_count::text AS order_count_value
            FROM live.l2_updates
            WHERE stream_key = $1
            "#,
        )
        .bind(stream_key)
        .fetch_one(&pool)
        .await
        .unwrap();

        // Zero means delete.
        assert_eq!(
            row.get::<Option<Decimal>, _>("quantity_base"),
            Some(Decimal::ZERO)
        );

        // NULL means unavailable, not delete.
        assert_eq!(row.get::<Option<Decimal>, _>("quantity_quote"), None);

        assert_eq!(row.get::<Option<Decimal>, _>("quantity_contracts"), None);

        assert_eq!(
            row.get::<String, _>("order_count_value"),
            u64::MAX.to_string()
        );

        cleanup_stream(&pool, stream_key).await;
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL test database"]
    async fn postgres_failed_batch_rolls_back_every_record() {
        let pool = test_pool().await;
        let instrument_id = existing_instrument(&pool).await;

        let stream_id = unique_id("marketforge-rollback-test");

        let stream_key = register_test_stream(&pool, &stream_id, instrument_id, 1).await;

        let writer = PostgresBatchWriter::new(pool.clone());

        let valid = RecordingEvent {
            stream_key,
            log_epoch: 1,
            event_offset: 1,
            event: trade(&stream_id, instrument_id),
        };

        let mut invalid_trade = match trade(&stream_id, instrument_id) {
            CanonicalEvent::Trade(trade) => trade,
            _ => unreachable!(),
        };

        invalid_trade.price = Decimal::new(-1, 0);

        let invalid = RecordingEvent {
            stream_key,
            log_epoch: 1,
            event_offset: 2,
            event: CanonicalEvent::Trade(invalid_trade),
        };

        let result = writer.write_batch(&[valid, invalid]).await;

        assert!(result.is_err());

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM live.trades WHERE stream_key = $1")
                .bind(stream_key)
                .fetch_one(&pool)
                .await
                .unwrap();

        // The first valid insert must have rolled back too.
        assert_eq!(count, 0);

        cleanup_stream(&pool, stream_key).await;
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL test database"]
    async fn postgres_duplicate_identity_is_rejected() {
        let pool = test_pool().await;
        let instrument_id = existing_instrument(&pool).await;

        let stream_id = unique_id("marketforge-duplicate-test");

        let stream_key = register_test_stream(&pool, &stream_id, instrument_id, 1).await;

        let writer = PostgresBatchWriter::new(pool.clone());

        let record = RecordingEvent {
            stream_key,
            log_epoch: 1,
            event_offset: 1,
            event: trade(&stream_id, instrument_id),
        };

        writer.write_batch(&[record.clone()]).await.unwrap();

        let result = writer.write_batch(&[record]).await;

        assert!(result.is_err());

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM live.trades WHERE stream_key = $1")
                .bind(stream_key)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(count, 1);

        cleanup_stream(&pool, stream_key).await;
    }
}
