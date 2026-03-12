use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use tuples_core::filter::Filter;

use crate::{FilterStore, InMemoryFilterStore};

pub struct PgFilterStore {
    pool: PgPool,
    cache: InMemoryFilterStore,
}

impl PgFilterStore {
    /// Create an empty store; call `load` to populate the cache from Postgres.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: InMemoryFilterStore::default(),
        }
    }

    /// Load all filters from Postgres into the in-memory cache.
    /// Call this once at server startup.
    pub async fn load(pool: PgPool) -> Result<Self> {
        let mut store = Self::new(pool);
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM filters ORDER BY id",
        )
        .fetch_all(&store.pool)
        .await?;
        for (data,) in rows {
            let filter: Filter = serde_json::from_value(data)?;
            store.cache.register(filter).await?;
        }
        Ok(store)
    }
}

#[async_trait]
impl FilterStore for PgFilterStore {
    async fn register(&mut self, filter: Filter) -> Result<()> {
        let data = serde_json::to_value(&filter)?;
        sqlx::query("INSERT INTO filters (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2")
            .bind(&filter.id)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        self.cache.register(filter).await
    }

    async fn get(&self, id: &str) -> Result<Option<Filter>> {
        self.cache.get(id).await
    }

    async fn list(&self) -> Result<Vec<Filter>> {
        self.cache.list().await
    }

    async fn match_data(&self, data: &Value) -> Result<Vec<String>> {
        self.cache.match_data(data).await
    }

    async fn clear(&mut self) -> Result<()> {
        sqlx::query("DELETE FROM filters").execute(&self.pool).await?;
        self.cache.clear().await
    }
}
