use anyhow::{anyhow, Result};
use async_trait::async_trait;
use foundationdb::{options::StreamingMode, Database, RangeOption};
use serde_json::Value;
use std::sync::Arc;
use tuples_core::filter::Filter;

use crate::{FilterStore, InMemoryFilterStore};

pub struct FdbFilterStore {
    db: Arc<Database>,
    cache: InMemoryFilterStore,
}

impl FdbFilterStore {
    /// Create an empty store; call `load` to populate the cache from FDB.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db, cache: InMemoryFilterStore::default() }
    }

    /// Load all filters from FDB into the in-memory cache.
    /// Call this once at server startup.
    pub async fn load(db: Arc<Database>) -> Result<Self> {
        let mut store = Self::new(db.clone());

        let (begin, end) = filters_prefix();
        let trx = db.create_trx()?;
        let range_opt = RangeOption {
            begin: foundationdb::KeySelector::first_greater_or_equal(begin),
            end: foundationdb::KeySelector::first_greater_or_equal(end),
            mode: StreamingMode::WantAll,
            reverse: false,
            ..Default::default()
        };
        let kvs = trx
            .get_range(&range_opt, 1, false)
            .await
            .map_err(|e| anyhow!("fdb range: {e}"))?;

        for kv in kvs.iter() {
            let filter: Filter = serde_json::from_slice(kv.value())?;
            store.cache.register(filter).await?;
        }

        Ok(store)
    }
}

fn filter_key(filter_id: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("filters", filter_id))
}

fn filters_prefix() -> (Vec<u8>, Vec<u8>) {
    let prefix = foundationdb::tuple::pack(&("filters",));
    let mut end = prefix.clone();
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

#[async_trait]
impl FilterStore for FdbFilterStore {
    async fn register(&mut self, filter: Filter) -> Result<()> {
        let key = filter_key(&filter.id);
        let value = serde_json::to_vec(&filter)?;
        let trx = self.db.create_trx()?;
        trx.set(&key, &value);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
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

#[cfg(feature = "fdb")]
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn order_filter() -> Filter {
        Filter {
            id: "fdb-f-order".to_string(),
            exact: [("type".to_string(), json!("order"))].into(),
            wildcards: vec![],
            predicates: vec![],
        }
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_filter_register_match_list() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbFilterStore::new(db.clone());
        store.register(order_filter()).await.unwrap();

        let data = json!({ "type": "order" });
        let ids = store.match_data(&data).await.unwrap();
        assert_eq!(ids, vec!["fdb-f-order"]);

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "fdb-f-order");
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_filter_cache_survives_reload() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbFilterStore::new(db.clone());
        store.register(order_filter()).await.unwrap();

        // Reload from FDB
        let reloaded = FdbFilterStore::load(db.clone()).await.unwrap();
        let list = reloaded.list().await.unwrap();
        let ids: Vec<_> = list.iter().map(|f| f.id.as_str()).collect();
        assert!(ids.contains(&"fdb-f-order"));

        let data = json!({ "type": "order" });
        let matched = reloaded.match_data(&data).await.unwrap();
        assert!(matched.contains(&"fdb-f-order".to_string()));
    }
}
