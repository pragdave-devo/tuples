use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tuples_core::run::{AgentRun, AgentStatus, PlaybookRun, RunStatus};

/// Storage for playbook run and agent run lifecycle state.
#[async_trait]
pub trait RunStore: Send + Sync {
    async fn create_run(&mut self, run: PlaybookRun) -> Result<()>;
    async fn get_run(&self, trace_id: &str) -> Result<Option<PlaybookRun>>;
    async fn update_run_status(&mut self, trace_id: &str, status: RunStatus) -> Result<()>;
    async fn list_runs(&self) -> Result<Vec<PlaybookRun>>;
    async fn create_agent_run(&mut self, run: AgentRun) -> Result<()>;
    async fn list_agent_runs(&self, trace_id: &str) -> Result<Vec<AgentRun>>;
    async fn update_agent_status(&mut self, id: &str, status: AgentStatus) -> Result<()>;
    /// Count of agent runs in Dispatched or Running state for the given trace_id.
    async fn active_agent_count(&self, trace_id: &str) -> Result<usize>;
    /// Remove all runs and agent runs.
    async fn clear(&mut self) -> Result<()>;
}

/// In-memory run store (run state is transient; FDB backend deferred).
#[derive(Default)]
pub struct InMemoryRunStore {
    runs: HashMap<String, PlaybookRun>,
    /// Keyed by AgentRun.id
    agent_runs: HashMap<String, AgentRun>,
}

#[async_trait]
impl RunStore for InMemoryRunStore {
    async fn create_run(&mut self, run: PlaybookRun) -> Result<()> {
        self.runs.insert(run.trace_id.clone(), run);
        Ok(())
    }

    async fn get_run(&self, trace_id: &str) -> Result<Option<PlaybookRun>> {
        Ok(self.runs.get(trace_id).cloned())
    }

    async fn update_run_status(&mut self, trace_id: &str, status: RunStatus) -> Result<()> {
        if let Some(run) = self.runs.get_mut(trace_id) {
            run.status = status;
        }
        Ok(())
    }

    async fn list_runs(&self) -> Result<Vec<PlaybookRun>> {
        let mut runs: Vec<PlaybookRun> = self.runs.values().cloned().collect();
        runs.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        Ok(runs)
    }

    async fn create_agent_run(&mut self, run: AgentRun) -> Result<()> {
        self.agent_runs.insert(run.id.clone(), run);
        Ok(())
    }

    async fn list_agent_runs(&self, trace_id: &str) -> Result<Vec<AgentRun>> {
        let mut runs: Vec<AgentRun> = self
            .agent_runs
            .values()
            .filter(|r| r.trace_id == trace_id)
            .cloned()
            .collect();
        runs.sort_by(|a, b| a.dispatched_at.cmp(&b.dispatched_at));
        Ok(runs)
    }

    async fn update_agent_status(&mut self, id: &str, status: AgentStatus) -> Result<()> {
        if let Some(run) = self.agent_runs.get_mut(id) {
            run.status = status;
        }
        Ok(())
    }

    async fn active_agent_count(&self, trace_id: &str) -> Result<usize> {
        let count = self
            .agent_runs
            .values()
            .filter(|r| r.trace_id == trace_id)
            .filter(|r| matches!(r.status, AgentStatus::Dispatched | AgentStatus::Running))
            .count();
        Ok(count)
    }

    async fn clear(&mut self) -> Result<()> {
        self.runs.clear();
        self.agent_runs.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn make_run(trace_id: &str) -> PlaybookRun {
        PlaybookRun {
            trace_id: trace_id.to_string(),
            playbook_name: "fulfil".to_string(),
            started_at: Utc::now(),
            status: RunStatus::Running,
        }
    }

    fn make_agent_run(id: &str, trace_id: &str) -> AgentRun {
        AgentRun {
            id: id.to_string(),
            trace_id: trace_id.to_string(),
            agent_name: "processor".to_string(),
            triggered_by: None,
            params: json!({}),
            status: AgentStatus::Dispatched,
            dispatched_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn create_and_get_run() {
        let mut store = InMemoryRunStore::default();
        store.create_run(make_run("trace-1")).await.unwrap();
        let run = store.get_run("trace-1").await.unwrap().unwrap();
        assert_eq!(run.playbook_name, "fulfil");
    }

    #[tokio::test]
    async fn get_run_missing_returns_none() {
        let store = InMemoryRunStore::default();
        assert!(store.get_run("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn update_run_status_completed() {
        let mut store = InMemoryRunStore::default();
        store.create_run(make_run("trace-1")).await.unwrap();
        store.update_run_status("trace-1", RunStatus::Completed).await.unwrap();
        let run = store.get_run("trace-1").await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Completed);
    }

    #[tokio::test]
    async fn list_runs_sorted_by_started_at() {
        let mut store = InMemoryRunStore::default();
        store.create_run(make_run("trace-2")).await.unwrap();
        store.create_run(make_run("trace-1")).await.unwrap();
        let runs = store.list_runs().await.unwrap();
        assert_eq!(runs.len(), 2);
    }

    #[tokio::test]
    async fn active_agent_count_tracks_dispatched_and_running() {
        let mut store = InMemoryRunStore::default();
        store.create_agent_run(make_agent_run("a1", "trace-1")).await.unwrap();
        store.create_agent_run(make_agent_run("a2", "trace-1")).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 2);

        store.update_agent_status("a1", AgentStatus::Completed).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 1);

        store.update_agent_status("a2", AgentStatus::Running).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 1);

        store.update_agent_status("a2", AgentStatus::Completed).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn list_agent_runs_filters_by_trace() {
        let mut store = InMemoryRunStore::default();
        store.create_agent_run(make_agent_run("a1", "trace-1")).await.unwrap();
        store.create_agent_run(make_agent_run("a2", "trace-2")).await.unwrap();
        let runs = store.list_agent_runs("trace-1").await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, "a1");
    }
}
