use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tuples_core::playbook::{Playbook, Trigger};

/// Persistent storage for playbooks.
#[async_trait]
pub trait PlaybookStore: Send + Sync {
    /// Register or overwrite a playbook.
    async fn register(&mut self, playbook: Playbook) -> Result<()>;
    /// Retrieve a playbook by name, or `None` if not found.
    async fn get(&self, name: &str) -> Result<Option<Playbook>>;
    /// List all registered playbooks, sorted by name.
    async fn list(&self) -> Result<Vec<Playbook>>;
    /// All (playbook_name, trigger) pairs across every playbook.
    async fn all_triggers(&self) -> Result<Vec<(String, Trigger)>>;
}

/// In-memory playbook store.
#[derive(Default)]
pub struct InMemoryPlaybookStore {
    playbooks: HashMap<String, Playbook>,
}

#[async_trait]
impl PlaybookStore for InMemoryPlaybookStore {
    async fn register(&mut self, playbook: Playbook) -> Result<()> {
        self.playbooks.insert(playbook.name.clone(), playbook);
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Option<Playbook>> {
        Ok(self.playbooks.get(name).cloned())
    }

    async fn list(&self) -> Result<Vec<Playbook>> {
        let mut playbooks: Vec<Playbook> = self.playbooks.values().cloned().collect();
        playbooks.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(playbooks)
    }

    async fn all_triggers(&self) -> Result<Vec<(String, Trigger)>> {
        let mut result = Vec::new();
        for playbook in self.playbooks.values() {
            for trigger in &playbook.triggers {
                result.push((playbook.name.clone(), trigger.clone()));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tuples_core::filter::Filter;
    use tuples_core::playbook::{
        ParamSource, Playbook, Trigger, TriggerExecution, TriggerMatch,
    };

    fn make_playbook(name: &str) -> Playbook {
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
    async fn register_and_get() {
        let mut store = InMemoryPlaybookStore::default();
        store.register(make_playbook("fulfil")).await.unwrap();
        let result = store.get("fulfil").await.unwrap();
        assert_eq!(result.unwrap().name, "fulfil");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = InMemoryPlaybookStore::default();
        assert!(store.get("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_sorted() {
        let mut store = InMemoryPlaybookStore::default();
        store.register(make_playbook("zebra")).await.unwrap();
        store.register(make_playbook("apple")).await.unwrap();
        store.register(make_playbook("mango")).await.unwrap();
        let names: Vec<_> =
            store.list().await.unwrap().into_iter().map(|p| p.name).collect();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    #[tokio::test]
    async fn all_triggers_aggregation() {
        let mut store = InMemoryPlaybookStore::default();
        store.register(make_playbook("fulfil")).await.unwrap();
        store.register(make_playbook("notify")).await.unwrap();
        let triggers = store.all_triggers().await.unwrap();
        assert_eq!(triggers.len(), 2);
        let names: std::collections::HashSet<_> =
            triggers.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains("fulfil"));
        assert!(names.contains("notify"));
    }
}
