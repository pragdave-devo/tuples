use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tuples_core::playbook::{Playbook, Trigger};

use crate::{InMemoryPlaybookStore, PlaybookStore};

pub struct PgPlaybookStore {
    pool: PgPool,
    cache: InMemoryPlaybookStore,
}

impl PgPlaybookStore {
    /// Create an empty store; call `load` to populate the cache from Postgres.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: InMemoryPlaybookStore::default(),
        }
    }

    /// Load all playbooks from Postgres into the in-memory cache.
    /// Call this once at server startup.
    pub async fn load(pool: PgPool) -> Result<Self> {
        let mut store = Self::new(pool);
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM playbooks ORDER BY name",
        )
        .fetch_all(&store.pool)
        .await?;
        for (data,) in rows {
            let playbook: Playbook = serde_json::from_value(data)?;
            store.cache.register(playbook).await?;
        }
        Ok(store)
    }
}

#[async_trait]
impl PlaybookStore for PgPlaybookStore {
    async fn register(&mut self, playbook: Playbook) -> Result<()> {
        let data = serde_json::to_value(&playbook)?;
        sqlx::query("INSERT INTO playbooks (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = $2")
            .bind(&playbook.name)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        self.cache.register(playbook).await
    }

    async fn get(&self, name: &str) -> Result<Option<Playbook>> {
        self.cache.get(name).await
    }

    async fn list(&self) -> Result<Vec<Playbook>> {
        self.cache.list().await
    }

    async fn all_triggers(&self) -> Result<Vec<(String, Trigger)>> {
        self.cache.all_triggers().await
    }
}
