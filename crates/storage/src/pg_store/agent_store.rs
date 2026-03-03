use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tuples_core::agent::Agent;

use crate::AgentStore;

pub struct PgAgentStore {
    pool: PgPool,
}

impl PgAgentStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AgentStore for PgAgentStore {
    async fn register(&mut self, agent: Agent) -> Result<()> {
        let data = serde_json::to_value(&agent)?;
        sqlx::query("INSERT INTO agents (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = $2")
            .bind(&agent.name)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Agent>> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM agents WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((data,)) => Ok(Some(serde_json::from_value(data)?)),
        }
    }

    async fn list(&self) -> Result<Vec<Agent>> {
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM agents ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_value(data)?))
            .collect()
    }
}
