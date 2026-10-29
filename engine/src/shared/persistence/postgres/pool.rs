use sqlx::{PgPool, postgres::PgPoolOptions};

use super::config::PostgresConfig;

pub async fn connect(config: &PostgresConfig) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(config.max_connections())
        .acquire_timeout(config.acquire_timeout())
        .connect(config.database_url())
        .await
}

pub async fn health_check(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool).await?;

    Ok(())
}

pub async fn shutdown(pool: &PgPool) {
    pool.close().await;
}
