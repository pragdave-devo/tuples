use anyhow::{anyhow, Result};
use async_trait::async_trait;
use foundationdb::Database;
use std::sync::Arc;
use tuples_core::tuple::Tuple;

use crate::TupleStore;

pub struct FdbTupleStore {
    db: Arc<Database>,
}

impl FdbTupleStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn tuple_key(uuid7: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("tuples", uuid7))
}

fn by_type_key(tuple_type: &str, uuid7: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("by_type", tuple_type, uuid7))
}

fn by_trace_key(trace_id: &str, uuid7: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("by_trace", trace_id, uuid7))
}

#[async_trait]
impl TupleStore for FdbTupleStore {
    async fn put(&mut self, tuple: Tuple) -> Result<()> {
        let primary_key = tuple_key(&tuple.uuid7);
        let by_type = by_type_key(&tuple.tuple_type, &tuple.uuid7);
        let by_trace = by_trace_key(&tuple.trace_id, &tuple.uuid7);
        let type_bytes = tuple.tuple_type.as_bytes().to_vec();

        let value = serde_json::to_vec(&tuple)?;

        let trx = self.db.create_trx()?;
        trx.set(&primary_key, &value);
        // Secondary indexes: by_type key holds a marker byte, by_trace holds the type
        trx.set(&by_type, b"");
        trx.set(&by_trace, &type_bytes);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }

    async fn get(&self, uuid7: &str) -> Result<Option<Tuple>> {
        let key = tuple_key(uuid7);
        let trx = self.db.create_trx()?;
        let result = trx.get(&key, false).await.map_err(|e| anyhow!("fdb get: {e}"))?;
        match result {
            None => Ok(None),
            Some(bytes) => {
                let tuple: Tuple = serde_json::from_slice(&bytes)?;
                Ok(Some(tuple))
            }
        }
    }
}

#[cfg(feature = "fdb")]
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn example_tuple(uuid7: &str) -> Tuple {
        Tuple {
            uuid7: uuid7.to_string(),
            trace_id: "trace-1".to_string(),
            created_at: Utc::now(),
            tuple_type: "order".to_string(),
            data: json!({ "id": "abc" }),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_tuple_put_get() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbTupleStore::new(db.clone());
        let t = example_tuple("test-uuid-1");
        store.put(t.clone()).await.unwrap();

        let got = store.get("test-uuid-1").await.unwrap().unwrap();
        assert_eq!(got.uuid7, "test-uuid-1");
        assert_eq!(got.tuple_type, "order");
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_tuple_secondary_index_keys_written() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbTupleStore::new(db.clone());
        let t = example_tuple("test-uuid-2");
        store.put(t).await.unwrap();

        // Verify by_type key exists
        let trx = db.create_trx().unwrap();
        let by_type = by_type_key("order", "test-uuid-2");
        let result = trx.get(&by_type, false).await.unwrap();
        assert!(result.is_some());

        // Verify by_trace key exists
        let by_trace = by_trace_key("trace-1", "test-uuid-2");
        let result = trx.get(&by_trace, false).await.unwrap();
        assert!(result.is_some());
    }
}
