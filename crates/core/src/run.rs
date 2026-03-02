use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Status of a playbook run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RunStatus {
    Running,
    Completed,
    Failed(String),
}

/// A playbook run instance, scoped to a trace_id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlaybookRun {
    pub trace_id: String,
    pub playbook_name: String,
    pub started_at: DateTime<Utc>,
    pub status: RunStatus,
}

/// Status of a single agent invocation within a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentStatus {
    Dispatched,
    Running,
    Completed,
    Failed(String),
}

/// A single agent invocation within a playbook run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRun {
    /// uuid7 identifier for this invocation.
    pub id: String,
    pub trace_id: String,
    pub agent_name: String,
    /// uuid7 of the tuple that caused dispatch; None for conductor (run start).
    pub triggered_by: Option<String>,
    pub params: Value,
    pub status: AgentStatus,
    pub dispatched_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_playbook_run() {
        let run = PlaybookRun {
            trace_id: "trace-1".to_string(),
            playbook_name: "fulfil".to_string(),
            started_at: DateTime::from_timestamp(0, 0).unwrap(),
            status: RunStatus::Running,
        };
        let json = serde_json::to_string(&run).unwrap();
        let back: PlaybookRun = serde_json::from_str(&json).unwrap();
        assert_eq!(run, back);
    }

    #[test]
    fn roundtrip_agent_run() {
        let run = AgentRun {
            id: "id-1".to_string(),
            trace_id: "trace-1".to_string(),
            agent_name: "processor".to_string(),
            triggered_by: None,
            params: json!({"order_id": "o1"}),
            status: AgentStatus::Dispatched,
            dispatched_at: DateTime::from_timestamp(0, 0).unwrap(),
        };
        let json_str = serde_json::to_string(&run).unwrap();
        let back: AgentRun = serde_json::from_str(&json_str).unwrap();
        assert_eq!(run, back);
    }

    #[test]
    fn run_status_failed_roundtrip() {
        let status = RunStatus::Failed("something went wrong".to_string());
        let json = serde_json::to_string(&status).unwrap();
        let back: RunStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back);
    }
}
