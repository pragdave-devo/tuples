mod agent_store;
mod filter_store;
mod playbook_store;
mod run_store;
mod schema_store;
mod tuple_store;

pub use agent_store::PgAgentStore;
pub use filter_store::PgFilterStore;
pub use playbook_store::PgPlaybookStore;
pub use run_store::PgRunStore;
pub use schema_store::PgSchemaStore;
pub use tuple_store::PgTupleStore;

/// Run the DDL migrations against the given pool.
pub async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let sql = include_str!("migrations/001_init.sql");
    sqlx::raw_sql(sql).execute(pool).await?;
    Ok(())
}
