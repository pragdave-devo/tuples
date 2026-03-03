use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tuples_core::schema::Schema;

use crate::SchemaStore;

pub struct PgSchemaStore {
    pool: PgPool,
}

impl PgSchemaStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SchemaStore for PgSchemaStore {
    async fn register(&mut self, schema: Schema) -> Result<()> {
        let data = serde_json::to_value(&schema)?;
        sqlx::query("INSERT INTO schemas (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = $2")
            .bind(&schema.name)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Schema>> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM schemas WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((data,)) => Ok(Some(serde_json::from_value(data)?)),
        }
    }

    async fn list(&self) -> Result<Vec<Schema>> {
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM schemas ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_value(data)?))
            .collect()
    }

    async fn clear(&mut self) -> Result<()> {
        anyhow::bail!("clear not implemented for Postgres backend")
    }
}
