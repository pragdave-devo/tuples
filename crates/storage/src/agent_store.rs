use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tuples_core::agent::Agent;

/// Persistent storage for global agents.
#[async_trait]
pub trait AgentStore: Send + Sync {
    async fn register(&mut self, agent: Agent) -> Result<()>;
    async fn get(&self, name: &str) -> Result<Option<Agent>>;
    async fn list(&self) -> Result<Vec<Agent>>;
}

/// In-memory agent store.
#[derive(Default)]
pub struct InMemoryAgentStore(HashMap<String, Agent>);

#[async_trait]
impl AgentStore for InMemoryAgentStore {
    async fn register(&mut self, agent: Agent) -> Result<()> {
        self.0.insert(agent.name.clone(), agent);
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Agent>> {
        Ok(self.0.get(name).cloned())
    }

    async fn list(&self) -> Result<Vec<Agent>> {
        let mut agents: Vec<Agent> = self.0.values().cloned().collect();
        agents.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(agents)
    }
}

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
    async fn register_and_get() {
        let mut store = InMemoryAgentStore::default();
        store.register(make_agent("processor")).await.unwrap();
        let result = store.get("processor").await.unwrap();
        assert_eq!(result.unwrap().name, "processor");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = InMemoryAgentStore::default();
        assert!(store.get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_sorted() {
        let mut store = InMemoryAgentStore::default();
        store.register(make_agent("zebra")).await.unwrap();
        store.register(make_agent("apple")).await.unwrap();
        store.register(make_agent("mango")).await.unwrap();
        let names: Vec<_> =
            store.list().await.unwrap().into_iter().map(|a| a.name).collect();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    #[tokio::test]
    async fn re_register_overwrites() {
        let mut store = InMemoryAgentStore::default();
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
