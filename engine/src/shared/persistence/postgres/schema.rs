use sqlx::{PgPool, migrate::Migrator};

pub async fn ensure_live_schema(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    let mut migrator: Migrator = sqlx::migrate!("./src/shared/persistence/postgres/migrations");

    migrator.create_schema("live");

    migrator.dangerous_set_table_name("live._sqlx_migrations");

    migrator.run(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_schema_contains_two_migrations() {
        let migrator: Migrator = sqlx::migrate!("./src/shared/persistence/postgres/migrations");

        assert_eq!(migrator.iter().count(), 2);
    }

    #[test]
    fn stream_definition_migration_creates_required_tables() {
        let sql = include_str!("migrations/0002_create_stream_definitions.sql");

        assert!(sql.contains("CREATE TABLE live.stream_definitions"));
        assert!(sql.contains("CREATE TABLE live.stream_dependencies"));
    }
}
