use anyhow::{anyhow, Result};
use async_trait::async_trait;
use foundationdb::{options::StreamingMode, Database, RangeOption};
use std::sync::Arc;
use tuples_core::playbook::{Playbook, Trigger};

use crate::{InMemoryPlaybookStore, PlaybookStore};

pub struct FdbPlaybookStore {
    db: Arc<Database>,
    cache: InMemoryPlaybookStore,
}

impl FdbPlaybookStore {
    /// Create an empty store; call `load` to populate the cache from FDB.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db, cache: InMemoryPlaybookStore::default() }
    }

    /// Load all playbooks from FDB into the in-memory cache.
    /// Call this once at server startup.
    pub async fn load(db: Arc<Database>) -> Result<Self> {
        let mut store = Self::new(db.clone());

        let (begin, end) = playbooks_prefix();
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
            let playbook: Playbook = serde_json::from_slice(kv.value())?;
            store.cache.register(playbook).await?;
        }

        Ok(store)
    }
}

fn playbook_key(name: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("playbooks", name))
}

fn playbooks_prefix() -> (Vec<u8>, Vec<u8>) {
    let prefix = foundationdb::tuple::pack(&("playbooks",));
    let mut end = prefix.clone();
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

#[async_trait]
impl PlaybookStore for FdbPlaybookStore {
    async fn register(&mut self, playbook: Playbook) -> Result<()> {
        let key = playbook_key(&playbook.name);
        let value = serde_json::to_vec(&playbook)?;
        let trx = self.db.create_trx()?;
        trx.set(&key, &value);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
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

    async fn clear(&mut self) -> Result<()> {
        let (begin, end) = playbooks_prefix();
        let trx = self.db.create_trx()?;
        trx.clear_range(&begin, &end);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        self.cache.clear().await
    }
}

#[cfg(feature = "fdb")]
#[cfg(test)]
mod tests {
    use super::*;
    use tuples_core::filter::Filter;
    use tuples_core::playbook::{
        ParamSource, Trigger, TriggerExecution, TriggerMatch,
    };

    fn example_playbook(name: &str) -> Playbook {
        Playbook {
            name: name.to_string(),
            description: "test".to_string(),
            conductor: "agent1".to_string(),
            agents: vec!["agent1".to_string()],
            triggers: vec![Trigger {
                id: format!("{name}-t1"),
                match_: TriggerMatch {
                    tuple_type: name.to_string(),
                    filter: Filter::default(),
                },
                execution: TriggerExecution {
                    agent: "agent1".to_string(),
                    params: [("order_id".to_string(), ParamSource::Field("id".to_string()))]
                        .into(),
                },
            }],
        }
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_playbook_register_get_list() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbPlaybookStore::new(db);
        store.register(example_playbook("fulfil")).await.unwrap();
        store.register(example_playbook("notify")).await.unwrap();

        let got = store.get("fulfil").await.unwrap().unwrap();
        assert_eq!(got.name, "fulfil");

        let list = store.list().await.unwrap();
        let names: Vec<_> = list.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"fulfil"));
        assert!(names.contains(&"notify"));
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_playbook_survives_reload() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbPlaybookStore::new(db.clone());
        store.register(example_playbook("fulfil")).await.unwrap();

        let reloaded = FdbPlaybookStore::load(db.clone()).await.unwrap();
        let list = reloaded.list().await.unwrap();
        let names: Vec<_> = list.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"fulfil"));

        let triggers = reloaded.all_triggers().await.unwrap();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].0, "fulfil");
    }
}
