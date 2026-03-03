use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use tuples_core::playbook::{Playbook, Trigger};

use crate::{InMemoryPlaybookStore, PlaybookStore};

pub struct DynamoPlaybookStore {
    client: Client,
    table: String,
    cache: InMemoryPlaybookStore,
}

impl DynamoPlaybookStore {
    /// Create an empty store; call `load` to populate the cache from DynamoDB.
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            table: format!("{prefix}playbooks"),
            cache: InMemoryPlaybookStore::default(),
        }
    }

    /// Load all playbooks from DynamoDB into the in-memory cache.
    /// Call this once at server startup.
    pub async fn load(client: Client, prefix: &str) -> Result<Self> {
        let mut store = Self::new(client, prefix);

        let result = store
            .client
            .scan()
            .table_name(&store.table)
            .send()
            .await?;
        for item in result.items() {
            let data = item
                .get("data")
                .and_then(|v| v.as_s().ok())
                .ok_or_else(|| anyhow::anyhow!("missing data attribute"))?;
            let playbook: Playbook = serde_json::from_str(data)?;
            store.cache.register(playbook).await?;
        }

        Ok(store)
    }
}

#[async_trait]
impl PlaybookStore for DynamoPlaybookStore {
    async fn register(&mut self, playbook: Playbook) -> Result<()> {
        let data = serde_json::to_string(&playbook)?;
        self.client
            .put_item()
            .table_name(&self.table)
            .item("name", AttributeValue::S(playbook.name.clone()))
            .item("data", AttributeValue::S(data))
            .send()
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
