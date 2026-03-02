use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;
use tuples_core::tuple::Tuple;

/// Persistent storage for tuples.
#[async_trait]
pub trait TupleStore: Send + Sync {
    /// Store a tuple. Overwrites any existing tuple with the same uuid7.
    async fn put(&self, tuple: Tuple) -> Result<()>;
    /// Retrieve a tuple by uuid7, or `None` if not found.
    async fn get(&self, uuid7: &str) -> Result<Option<Tuple>>;
    /// Store multiple tuples atomically. Default implementation calls `put` sequentially.
    async fn put_batch(&self, tuples: &[Tuple]) -> Result<()> {
        for t in tuples {
            self.put(t.clone()).await?;
        }
        Ok(())
    }
}

/// In-memory tuple store (for testing and early stages).
#[derive(Default)]
pub struct InMemoryTupleStore {
    tuples: RwLock<HashMap<String, Tuple>>,
}

#[async_trait]
impl TupleStore for InMemoryTupleStore {
    async fn put(&self, tuple: Tuple) -> Result<()> {
        self.tuples.write().unwrap().insert(tuple.uuid7.clone(), tuple);
        Ok(())
    }

    async fn get(&self, uuid7: &str) -> Result<Option<Tuple>> {
        Ok(self.tuples.read().unwrap().get(uuid7).cloned())
    }
}

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
    async fn put_and_get() {
        let store = InMemoryTupleStore::default();
        let t = example_tuple("id-1");
        store.put(t.clone()).await.unwrap();
        let result = store.get("id-1").await.unwrap();
        assert_eq!(result.unwrap().uuid7, "id-1");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = InMemoryTupleStore::default();
        assert!(store.get("nope").await.unwrap().is_none());
    }
}
