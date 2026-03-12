use anyhow::{anyhow, Result};
use async_trait::async_trait;
use foundationdb::{options::StreamingMode, Database, RangeOption};
use std::sync::Arc;
use tuples_core::agent::Agent;

use crate::AgentStore;

pub struct FdbAgentStore {
    db: Arc<Database>,
}

impl FdbAgentStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn agent_key(name: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("agents", name))
}

fn agents_prefix() -> (Vec<u8>, Vec<u8>) {
    let prefix = foundationdb::tuple::pack(&("agents",));
    let mut end = prefix.clone();
    // Increment last byte to form exclusive end of range
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

#[async_trait]
impl AgentStore for FdbAgentStore {
    async fn register(&mut self, agent: Agent) -> Result<()> {
        let key = agent_key(&agent.name);
        let value = serde_json::to_vec(&agent)?;
        let trx = self.db.create_trx()?;
        trx.set(&key, &value);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Agent>> {
        let key = agent_key(name);
        let trx = self.db.create_trx()?;
        let result = trx.get(&key, false).await.map_err(|e| anyhow!("fdb get: {e}"))?;
        match result {
            None => Ok(None),
            Some(bytes) => {
                let agent: Agent = serde_json::from_slice(&bytes)?;
                Ok(Some(agent))
            }
        }
    }

    async fn list(&self) -> Result<Vec<Agent>> {
        let (begin, end) = agents_prefix();
        let trx = self.db.create_trx()?;
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
        let mut agents: Vec<Agent> = kvs
            .iter()
            .map(|kv| serde_json::from_slice(kv.value()))
            .collect::<Result<_, _>>()?;
        agents.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(agents)
    }

    async fn clear(&mut self) -> Result<()> {
        let (begin, end) = agents_prefix();
        let trx = self.db.create_trx()?;
        trx.clear_range(&begin, &end);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }
}

#[cfg(feature = "fdb")]
#[cfg(test)]
mod tests {
    use super::*;

    fn make_agent(name: &str) -> Agent {
        Agent {
            name: name.to_string(),
            description: "test agent".to_string(),
            schema: "order".to_string(),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_agent_register_get_list() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbAgentStore::new(db);
        store.register(make_agent("processor")).await.unwrap();
        store.register(make_agent("notifier")).await.unwrap();

        let got = store.get("processor").await.unwrap().unwrap();
        assert_eq!(got.name, "processor");

        let list = store.list().await.unwrap();
        let names: Vec<_> = list.iter().map(|a| a.name.as_str()).collect();
        assert!(names.contains(&"processor"));
        assert!(names.contains(&"notifier"));
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_agent_get_missing_returns_none() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let store = FdbAgentStore::new(db);
        assert!(store.get("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_agent_re_register_overwrites() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbAgentStore::new(db);
        store.register(make_agent("processor")).await.unwrap();
        store
            .register(Agent {
                name: "processor".to_string(),
                description: "updated".to_string(),
                schema: "order".to_string(),
            })
            .await
            .unwrap();
        let agent = store.get("processor").await.unwrap().unwrap();
        assert_eq!(agent.description, "updated");
    }
}
