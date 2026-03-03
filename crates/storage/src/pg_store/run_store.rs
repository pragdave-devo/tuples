use anyhow::{anyhow, Result};
use async_trait::async_trait;
use sqlx::PgPool;
use tuples_core::run::{AgentRun, AgentStatus, PlaybookRun, RunStatus};

use crate::RunStore;

pub struct PgRunStore {
    pool: PgPool,
}

impl PgRunStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RunStore for PgRunStore {
    async fn create_run(&mut self, run: PlaybookRun) -> Result<()> {
        let data = serde_json::to_value(&run)?;
        sqlx::query("INSERT INTO runs (trace_id, data) VALUES ($1, $2)")
            .bind(&run.trace_id)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_run(&self, trace_id: &str) -> Result<Option<PlaybookRun>> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM runs WHERE trace_id = $1",
        )
        .bind(trace_id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((data,)) => Ok(Some(serde_json::from_value(data)?)),
        }
    }

    async fn update_run_status(&mut self, trace_id: &str, status: RunStatus) -> Result<()> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM runs WHERE trace_id = $1",
        )
        .bind(trace_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((data,)) = row {
            let mut run: PlaybookRun = serde_json::from_value(data)?;
            run.status = status;
            let updated = serde_json::to_value(&run)?;
            sqlx::query("UPDATE runs SET data = $1 WHERE trace_id = $2")
                .bind(&updated)
                .bind(trace_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn list_runs(&self) -> Result<Vec<PlaybookRun>> {
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM runs ORDER BY data->>'started_at'",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_value(data)?))
            .collect()
    }

    async fn create_agent_run(&mut self, run: AgentRun) -> Result<()> {
        let data = serde_json::to_value(&run)?;
        sqlx::query("INSERT INTO agent_runs (id, trace_id, data) VALUES ($1, $2, $3)")
            .bind(&run.id)
            .bind(&run.trace_id)
            .bind(&data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_agent_runs(&self, trace_id: &str) -> Result<Vec<AgentRun>> {
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM agent_runs WHERE trace_id = $1 ORDER BY data->>'dispatched_at'",
        )
        .bind(trace_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_value(data)?))
            .collect()
    }

    async fn update_agent_status(&mut self, id: &str, status: AgentStatus) -> Result<()> {
        let row = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT data FROM agent_runs WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((data,)) = row {
            let mut run: AgentRun = serde_json::from_value(data)?;
            run.status = status;
            let updated = serde_json::to_value(&run)?;
            sqlx::query("UPDATE agent_runs SET data = $1 WHERE id = $2")
                .bind(&updated)
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn active_agent_count(&self, trace_id: &str) -> Result<usize> {
        let row = sqlx::query_as::<_, (i64,)>(
            "SELECT COUNT(*) FROM agent_runs WHERE trace_id = $1 AND data->>'status' IN ('Dispatched', 'Running')",
        )
        .bind(trace_id)
        .fetch_one(&self.pool)
        .await?;
        let count: usize = row.0.try_into().map_err(|_| anyhow!("negative count"))?;
        Ok(count)
    }
}
