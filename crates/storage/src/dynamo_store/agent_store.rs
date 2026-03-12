use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use tuples_core::agent::Agent;

use crate::AgentStore;

pub struct DynamoAgentStore {
    client: Client,
    table: String,
}

impl DynamoAgentStore {
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            table: format!("{prefix}agents"),
        }
    }
}

#[async_trait]
impl AgentStore for DynamoAgentStore {
    async fn register(&mut self, agent: Agent) -> Result<()> {
        let data = serde_json::to_string(&agent)?;
        self.client
            .put_item()
            .table_name(&self.table)
            .item("name", AttributeValue::S(agent.name.clone()))
            .item("data", AttributeValue::S(data))
            .send()
            .await?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Agent>> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("name", AttributeValue::S(name.to_string()))
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

    async fn list(&self) -> Result<Vec<Agent>> {
        let result = self.client.scan().table_name(&self.table).send().await?;
        let mut items = Vec::new();
        for item in result.items() {
            let data = item
                .get("data")
                .and_then(|v| v.as_s().ok())
                .ok_or_else(|| anyhow::anyhow!("missing data attribute"))?;
            items.push(serde_json::from_str(data)?);
        }
        items.sort_by(|a: &Agent, b: &Agent| a.name.cmp(&b.name));
        Ok(items)
    }

    async fn clear(&mut self) -> Result<()> {
        super::clear_table(&self.client, &self.table, "name").await
    }
}
