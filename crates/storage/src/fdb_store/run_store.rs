use anyhow::{anyhow, Result};
use async_trait::async_trait;
use foundationdb::{options::StreamingMode, Database, RangeOption};
use std::sync::Arc;
use tuples_core::run::{AgentRun, AgentStatus, PlaybookRun, RunStatus};

use crate::RunStore;

pub struct FdbRunStore {
    db: Arc<Database>,
}

impl FdbRunStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

// Key: ("runs", trace_id) -> PlaybookRun JSON
fn run_key(trace_id: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("runs", trace_id))
}

fn runs_prefix() -> (Vec<u8>, Vec<u8>) {
    let prefix = foundationdb::tuple::pack(&("runs",));
    let mut end = prefix.clone();
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

// Key: ("agent_runs", id) -> AgentRun JSON
fn agent_run_key(id: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("agent_runs", id))
}

// Secondary index: ("agent_runs_by_trace", trace_id, id) -> empty marker
fn agent_run_by_trace_key(trace_id: &str, id: &str) -> Vec<u8> {
    foundationdb::tuple::pack(&("agent_runs_by_trace", trace_id, id))
}

fn agent_runs_by_trace_prefix(trace_id: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = foundationdb::tuple::pack(&("agent_runs_by_trace", trace_id));
    let mut end = prefix.clone();
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

#[async_trait]
impl RunStore for FdbRunStore {
    async fn create_run(&mut self, run: PlaybookRun) -> Result<()> {
        let key = run_key(&run.trace_id);
        let value = serde_json::to_vec(&run)?;
        let trx = self.db.create_trx()?;
        trx.set(&key, &value);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }

    async fn get_run(&self, trace_id: &str) -> Result<Option<PlaybookRun>> {
        let key = run_key(trace_id);
        let trx = self.db.create_trx()?;
        let result = trx.get(&key, false).await.map_err(|e| anyhow!("fdb get: {e}"))?;
        match result {
            None => Ok(None),
            Some(bytes) => {
                let run: PlaybookRun = serde_json::from_slice(&bytes)?;
                Ok(Some(run))
            }
        }
    }

    async fn update_run_status(&mut self, trace_id: &str, status: RunStatus) -> Result<()> {
        let key = run_key(trace_id);
        let trx = self.db.create_trx()?;
        let result = trx.get(&key, false).await.map_err(|e| anyhow!("fdb get: {e}"))?;
        if let Some(bytes) = result {
            let mut run: PlaybookRun = serde_json::from_slice(&bytes)?;
            run.status = status;
            let value = serde_json::to_vec(&run)?;
            trx.set(&key, &value);
            trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        }
        Ok(())
    }

    async fn list_runs(&self) -> Result<Vec<PlaybookRun>> {
        let (begin, end) = runs_prefix();
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
        let mut runs: Vec<PlaybookRun> = kvs
            .iter()
            .map(|kv| serde_json::from_slice(kv.value()))
            .collect::<Result<_, _>>()?;
        runs.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        Ok(runs)
    }

    async fn create_agent_run(&mut self, run: AgentRun) -> Result<()> {
        let primary_key = agent_run_key(&run.id);
        let index_key = agent_run_by_trace_key(&run.trace_id, &run.id);
        let value = serde_json::to_vec(&run)?;
        let trx = self.db.create_trx()?;
        trx.set(&primary_key, &value);
        trx.set(&index_key, &[]);
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }

    async fn list_agent_runs(&self, trace_id: &str) -> Result<Vec<AgentRun>> {
        let (begin, end) = agent_runs_by_trace_prefix(trace_id);
        let trx = self.db.create_trx()?;

        // Scan the secondary index to get agent run ids
        let range_opt = RangeOption {
            begin: foundationdb::KeySelector::first_greater_or_equal(begin),
            end: foundationdb::KeySelector::first_greater_or_equal(end),
            mode: StreamingMode::WantAll,
            reverse: false,
            ..Default::default()
        };
        let index_kvs = trx
            .get_range(&range_opt, 1, false)
            .await
            .map_err(|e| anyhow!("fdb range: {e}"))?;

        // Collect ids first to avoid holding non-Send iterator across await
        let ids: Vec<String> = index_kvs
            .iter()
            .map(|kv| {
                let elements: (String, String, String) =
                    foundationdb::tuple::unpack(kv.key()).map_err(|e| anyhow!("fdb unpack: {e}"))?;
                Ok(elements.2)
            })
            .collect::<Result<_>>()?;

        // Look up each agent run by id
        let mut runs = Vec::new();
        for id in &ids {
            let primary_key = agent_run_key(id);
            let result = trx
                .get(&primary_key, false)
                .await
                .map_err(|e| anyhow!("fdb get: {e}"))?;
            if let Some(bytes) = result {
                let agent_run: AgentRun = serde_json::from_slice(&bytes)?;
                runs.push(agent_run);
            }
        }
        runs.sort_by(|a, b| a.dispatched_at.cmp(&b.dispatched_at));
        Ok(runs)
    }

    async fn update_agent_status(&mut self, id: &str, status: AgentStatus) -> Result<()> {
        let key = agent_run_key(id);
        let trx = self.db.create_trx()?;
        let result = trx.get(&key, false).await.map_err(|e| anyhow!("fdb get: {e}"))?;
        if let Some(bytes) = result {
            let mut run: AgentRun = serde_json::from_slice(&bytes)?;
            run.status = status;
            let value = serde_json::to_vec(&run)?;
            trx.set(&key, &value);
            trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        }
        Ok(())
    }

    async fn active_agent_count(&self, trace_id: &str) -> Result<usize> {
        let runs = self.list_agent_runs(trace_id).await?;
        let count = runs
            .iter()
            .filter(|r| matches!(r.status, AgentStatus::Dispatched | AgentStatus::Running))
            .count();
        Ok(count)
    }

    async fn clear(&mut self) -> Result<()> {
        let trx = self.db.create_trx()?;
        for prefix_tag in &["runs", "agent_runs", "agent_runs_by_trace"] {
            let prefix = foundationdb::tuple::pack(&(*prefix_tag,));
            let mut end = prefix.clone();
            *end.last_mut().unwrap() += 1;
            trx.clear_range(&prefix, &end);
        }
        trx.commit().await.map_err(|e| anyhow!("fdb commit: {e}"))?;
        Ok(())
    }
}

#[cfg(feature = "fdb")]
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
    #[ignore]
    async fn fdb_run_create_and_get() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbRunStore::new(db);
        store.create_run(make_run("trace-1")).await.unwrap();
        let run = store.get_run("trace-1").await.unwrap().unwrap();
        assert_eq!(run.playbook_name, "fulfil");
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_run_get_missing_returns_none() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let store = FdbRunStore::new(db);
        assert!(store.get_run("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_run_update_status() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbRunStore::new(db);
        store.create_run(make_run("trace-1")).await.unwrap();
        store.update_run_status("trace-1", RunStatus::Completed).await.unwrap();
        let run = store.get_run("trace-1").await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Completed);
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_run_list_runs() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbRunStore::new(db);
        store.create_run(make_run("trace-a")).await.unwrap();
        store.create_run(make_run("trace-b")).await.unwrap();
        let runs = store.list_runs().await.unwrap();
        assert!(runs.len() >= 2);
    }

    #[tokio::test]
    #[ignore]
    async fn fdb_run_agent_runs_lifecycle() {
        let _network = unsafe { foundationdb::boot() };
        let db = Arc::new(Database::default().unwrap());

        let mut store = FdbRunStore::new(db);
        store.create_agent_run(make_agent_run("a1", "trace-1")).await.unwrap();
        store.create_agent_run(make_agent_run("a2", "trace-1")).await.unwrap();
        store.create_agent_run(make_agent_run("a3", "trace-2")).await.unwrap();

        // list_agent_runs filters by trace_id
        let runs = store.list_agent_runs("trace-1").await.unwrap();
        assert_eq!(runs.len(), 2);

        // active_agent_count tracks dispatched and running
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 2);

        store.update_agent_status("a1", AgentStatus::Completed).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 1);

        store.update_agent_status("a2", AgentStatus::Running).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 1);

        store.update_agent_status("a2", AgentStatus::Completed).await.unwrap();
        assert_eq!(store.active_agent_count("trace-1").await.unwrap(), 0);
    }
}
