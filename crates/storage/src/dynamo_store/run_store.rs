use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use tuples_core::run::{AgentRun, AgentStatus, PlaybookRun, RunStatus};

use crate::RunStore;

pub struct DynamoRunStore {
    client: Client,
    runs_table: String,
    agent_runs_table: String,
}

impl DynamoRunStore {
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            runs_table: format!("{prefix}runs"),
            agent_runs_table: format!("{prefix}agent_runs"),
        }
    }
}

#[async_trait]
impl RunStore for DynamoRunStore {
    async fn create_run(&mut self, run: PlaybookRun) -> Result<()> {
        let data = serde_json::to_string(&run)?;
        self.client
            .put_item()
            .table_name(&self.runs_table)
            .item("trace_id", AttributeValue::S(run.trace_id.clone()))
            .item("data", AttributeValue::S(data))
            .send()
            .await?;
        Ok(())
    }

    async fn get_run(&self, trace_id: &str) -> Result<Option<PlaybookRun>> {
        let result = self
            .client
            .get_item()
            .table_name(&self.runs_table)
            .key("trace_id", AttributeValue::S(trace_id.to_string()))
            .send()
            .await?;
        match result.item {
            None => Ok(None),
            Some(item) => {
                let data = item
                    .get("data")
                    .and_then(|v| v.as_s().ok())
                    .ok_or_else(|| anyhow::anyhow!("missing data attribute"))?;
                Ok(Some(serde_json::from_str(data)?))
            }
        }
    }

    async fn update_run_status(&mut self, trace_id: &str, status: RunStatus) -> Result<()> {
        let existing = self
            .get_run(trace_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("run not found: {trace_id}"))?;
        let updated = PlaybookRun { status, ..existing };
        let data = serde_json::to_string(&updated)?;
        self.client
            .put_item()
            .table_name(&self.runs_table)
            .item("trace_id", AttributeValue::S(trace_id.to_string()))
            .item("data", AttributeValue::S(data))
            .send()
            .await?;
        Ok(())
    }

    async fn list_runs(&self) -> Result<Vec<PlaybookRun>> {
        let result = self
            .client
            .scan()
            .table_name(&self.runs_table)
            .send()
            .await?;
        let mut items = Vec::new();
        for item in result.items() {
            let data = item
                .get("data")
                .and_then(|v| v.as_s().ok())
                .ok_or_else(|| anyhow::anyhow!("missing data attribute"))?;
            items.push(serde_json::from_str(data)?);
        }
        items.sort_by(|a: &PlaybookRun, b: &PlaybookRun| a.started_at.cmp(&b.started_at));
        Ok(items)
    }

    async fn create_agent_run(&mut self, run: AgentRun) -> Result<()> {
        let data = serde_json::to_string(&run)?;
        self.client
            .put_item()
            .table_name(&self.agent_runs_table)
            .item("id", AttributeValue::S(run.id.clone()))
            .item("trace_id", AttributeValue::S(run.trace_id.clone()))
            .item("data", AttributeValue::S(data))
            .send()
            .await?;
        Ok(())
    }

    async fn list_agent_runs(&self, trace_id: &str) -> Result<Vec<AgentRun>> {
        let result = self
            .client
            .query()
            .table_name(&self.agent_runs_table)
            .index_name("by_trace")
            .key_condition_expression("trace_id = :tid")
            .expression_attribute_values(":tid", AttributeValue::S(trace_id.to_string()))
            .send()
            .await?;
        let mut items = Vec::new();
        for item in result.items() {
            let data = item
                .get("data")
                .and_then(|v| v.as_s().ok())
                .ok_or_else(|| anyhow::anyhow!("missing data attribute"))?;
            items.push(serde_json::from_str(data)?);
        }
        items.sort_by(|a: &AgentRun, b: &AgentRun| a.dispatched_at.cmp(&b.dispatched_at));
        Ok(items)
    }

    async fn update_agent_status(&mut self, id: &str, status: AgentStatus) -> Result<()> {
        // Get the existing agent run by primary key
        let result = self
            .client
            .get_item()
            .table_name(&self.agent_runs_table)
            .key("id", AttributeValue::S(id.to_string()))
            .send()
            .await?;
        let item = result
            .item
            .ok_or_else(|| anyhow::anyhow!("agent run not found: {id}"))?;
        let data = item
            .get("data")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| anyhow::anyhow!("missing data attribute"))?;
        let existing: AgentRun = serde_json::from_str(data)?;
        let updated = AgentRun { status, ..existing };
        let updated_data = serde_json::to_string(&updated)?;
        self.client
            .put_item()
            .table_name(&self.agent_runs_table)
            .item("id", AttributeValue::S(updated.id.clone()))
            .item("trace_id", AttributeValue::S(updated.trace_id.clone()))
            .item("data", AttributeValue::S(updated_data))
            .send()
            .await?;
        Ok(())
    }

    async fn active_agent_count(&self, trace_id: &str) -> Result<usize> {
        let runs = self.list_agent_runs(trace_id).await?;
        let count = runs
            .iter()
            .filter(|r| matches!(r.status, AgentStatus::Dispatched | AgentStatus::Running))
            .count();
        Ok(count)
    }

    async fn clear(&mut self) -> Result<()> {
        anyhow::bail!("clear not implemented for DynamoDB backend")
    }
}
