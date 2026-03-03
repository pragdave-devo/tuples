use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use serde_json::Value;
use tuples_core::filter::Filter;

use crate::{FilterStore, InMemoryFilterStore};

pub struct DynamoFilterStore {
    client: Client,
    table: String,
    cache: InMemoryFilterStore,
}

impl DynamoFilterStore {
    /// Create an empty store; call `load` to populate the cache from DynamoDB.
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            table: format!("{prefix}filters"),
            cache: InMemoryFilterStore::default(),
        }
    }

    /// Load all filters from DynamoDB into the in-memory cache.
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
            let filter: Filter = serde_json::from_str(data)?;
            store.cache.register(filter).await?;
        }

        Ok(store)
    }
}

#[async_trait]
impl FilterStore for DynamoFilterStore {
    async fn register(&mut self, filter: Filter) -> Result<()> {
        let data = serde_json::to_string(&filter)?;
        self.client
            .put_item()
            .table_name(&self.table)
            .item("id", AttributeValue::S(filter.id.clone()))
            .item("data", AttributeValue::S(data))
            .send()
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
}
