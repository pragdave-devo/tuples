use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::{AttributeValue, WriteRequest};
use aws_sdk_dynamodb::Client;
use tuples_core::tuple::Tuple;

use crate::tuple_store::BatchConfig;
use crate::TupleStore;

pub struct DynamoTupleStore {
    client: Client,
    table: String,
}

impl DynamoTupleStore {
    pub fn new(client: Client, prefix: &str) -> Self {
        Self {
            client,
            table: format!("{prefix}tuples"),
        }
    }

    fn put_request(tuple: &Tuple) -> Result<WriteRequest> {
        let data = serde_json::to_string(tuple)?;
        let put_request = aws_sdk_dynamodb::types::PutRequest::builder()
            .item("uuid7", AttributeValue::S(tuple.uuid7.clone()))
            .item("tuple_type", AttributeValue::S(tuple.tuple_type.clone()))
            .item("trace_id", AttributeValue::S(tuple.trace_id.clone()))
            .item("data", AttributeValue::S(data))
            .build()?;
        Ok(WriteRequest::builder().put_request(put_request).build())
    }
}

#[async_trait]
impl TupleStore for DynamoTupleStore {
    fn batch_config(&self) -> BatchConfig {
        BatchConfig::DYNAMODB
    }

    async fn put(&self, tuple: Tuple) -> Result<()> {
        let data = serde_json::to_string(&tuple)?;
        self.client
            .put_item()
            .table_name(&self.table)
            .item("uuid7", AttributeValue::S(tuple.uuid7.clone()))
            .item("tuple_type", AttributeValue::S(tuple.tuple_type.clone()))
            .item("trace_id", AttributeValue::S(tuple.trace_id.clone()))
            .item("data", AttributeValue::S(data))
            .send()
            .await?;
        Ok(())
    }

    async fn get(&self, uuid7: &str) -> Result<Option<Tuple>> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("uuid7", AttributeValue::S(uuid7.to_string()))
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

    async fn put_batch(&self, tuples: &[Tuple]) -> Result<()> {
        let mut futs = Vec::new();
        for chunk in tuples.chunks(25) {
            let mut requests = Vec::with_capacity(chunk.len());
            for tuple in chunk {
                requests.push(Self::put_request(tuple)?);
            }
            futs.push(
                self.client
                    .batch_write_item()
                    .request_items(&self.table, requests)
                    .send(),
            );
        }
        futures::future::try_join_all(futs).await?;
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        super::clear_table(&self.client, &self.table, "uuid7").await
    }
}
