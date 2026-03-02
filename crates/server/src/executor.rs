use chrono::Utc;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tonic::Status;
use uuid::Uuid;

use proto::tuples::{
    run_event, AgentCompletedEvent, AgentDispatchedEvent, AgentStartedEvent, RunCompletedEvent,
    RunEvent, TriggerFiredEvent,
};
use tuples_core::playbook::ParamSource;
use tuples_core::run::{AgentRun, AgentStatus, PlaybookRun, RunStatus};
use tuples_core::tuple::Tuple;
use tuples_storage::{AgentStore, PlaybookStore, RunStore, SchemaStore};

use crate::batch_writer::BatchWriter;

pub struct Executor {
    playbooks: Arc<Mutex<dyn PlaybookStore>>,
    agents: Arc<Mutex<dyn AgentStore>>,
    schemas: Arc<Mutex<dyn SchemaStore>>,
    runs: Arc<Mutex<dyn RunStore>>,
    batch_writer: Arc<BatchWriter>,
    pub run_tx: broadcast::Sender<RunEvent>,
    pub dispatch_tx: broadcast::Sender<AgentDispatchedEvent>,
}

impl Executor {
    pub fn new(
        playbooks: Arc<Mutex<dyn PlaybookStore>>,
        agents: Arc<Mutex<dyn AgentStore>>,
        schemas: Arc<Mutex<dyn SchemaStore>>,
        runs: Arc<Mutex<dyn RunStore>>,
        batch_writer: Arc<BatchWriter>,
        trigger_tx: broadcast::Sender<TriggerFiredEvent>,
        run_tx: broadcast::Sender<RunEvent>,
        dispatch_tx: broadcast::Sender<AgentDispatchedEvent>,
    ) -> Self {
        // Spawn background trigger-to-dispatch bridge.
        let bridge_playbooks = Arc::clone(&playbooks);
        let bridge_runs = Arc::clone(&runs);
        let bridge_run_tx = run_tx.clone();
        let bridge_dispatch_tx = dispatch_tx.clone();
        let trigger_rx = trigger_tx.subscribe();
        tokio::spawn(trigger_bridge(
            trigger_rx,
            bridge_playbooks,
            bridge_runs,
            bridge_run_tx,
            bridge_dispatch_tx,
        ));

        Self { playbooks, agents, schemas, runs, batch_writer, run_tx, dispatch_tx }
    }

    /// Start a playbook run:
    /// 1. Validate params against conductor's schema.
    /// 2. Create a PlaybookRun record.
    /// 3. Write an `initial_parameters` tuple for auditability.
    /// 4. Dispatch the conductor agent directly.
    /// Returns the new `trace_id`.
    pub async fn run_playbook(
        &self,
        playbook_name: &str,
        params: serde_json::Value,
    ) -> Result<String, Status> {
        // 1. Look up playbook and conductor agent.
        let playbook = self
            .playbooks
            .lock()
            .await
            .get(playbook_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!("playbook '{playbook_name}' not found"))
            })?;

        let conductor_name = playbook.conductor.clone();
        let conductor = self
            .agents
            .lock()
            .await
            .get(&conductor_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!("conductor agent '{conductor_name}' not found"))
            })?;

        // 2. Validate params against conductor's schema.
        let schema = self
            .schemas
            .lock()
            .await
            .get(&conductor.schema)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!("schema '{}' not found", conductor.schema))
            })?;
        schema
            .validate(&params)
            .map_err(|e| Status::invalid_argument(format!("params validation failed: {e}")))?;

        // 3. Create run record.
        let trace_id = Uuid::now_v7().to_string();
        self.runs
            .lock()
            .await
            .create_run(PlaybookRun {
                trace_id: trace_id.clone(),
                playbook_name: playbook_name.to_string(),
                started_at: Utc::now(),
                status: RunStatus::Running,
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // 4. Write initial_parameters tuple for auditability.
        let mut data_map = params.as_object().cloned().unwrap_or_default();
        data_map.insert(
            "_agent_name".to_string(),
            serde_json::Value::String(conductor_name.clone()),
        );
        let init_tuple = Tuple {
            uuid7: Uuid::now_v7().to_string(),
            trace_id: trace_id.clone(),
            created_at: Utc::now(),
            tuple_type: "initial_parameters".to_string(),
            data: serde_json::Value::Object(data_map),
        };
        self.batch_writer.put(init_tuple, true).await?;

        // 5. Dispatch conductor directly.
        self.dispatch_agent(&trace_id, &conductor_name, params, None).await?;

        Ok(trace_id)
    }

    /// Create an AgentRun record and broadcast dispatch events on both channels.
    pub async fn dispatch_agent(
        &self,
        trace_id: &str,
        agent_name: &str,
        params: serde_json::Value,
        triggered_by: Option<String>,
    ) -> Result<(), Status> {
        let id = Uuid::now_v7().to_string();
        self.runs
            .lock()
            .await
            .create_agent_run(AgentRun {
                id: id.clone(),
                trace_id: trace_id.to_string(),
                agent_name: agent_name.to_string(),
                triggered_by,
                params: params.clone(),
                status: AgentStatus::Dispatched,
                dispatched_at: Utc::now(),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let dispatched = AgentDispatchedEvent {
            agent_run_id: id,
            agent_name: agent_name.to_string(),
            params: params.to_string(),
        };

        // Broadcast as a RunEvent for WatchRun subscribers.
        let run_event = RunEvent {
            trace_id: trace_id.to_string(),
            event: Some(run_event::Event::AgentDispatched(dispatched.clone())),
        };
        let _ = self.run_tx.send(run_event);

        // Broadcast raw dispatch event for WatchAgentDispatch subscribers.
        let _ = self.dispatch_tx.send(dispatched);

        Ok(())
    }

    /// Record that an agent has started executing.
    pub async fn notify_agent_started(
        &self,
        trace_id: &str,
        agent_run_id: &str,
    ) -> Result<(), Status> {
        self.runs
            .lock()
            .await
            .update_agent_status(agent_run_id, AgentStatus::Running)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let _ = self.run_tx.send(RunEvent {
            trace_id: trace_id.to_string(),
            event: Some(run_event::Event::AgentStarted(AgentStartedEvent {
                agent_run_id: agent_run_id.to_string(),
            })),
        });
        Ok(())
    }

    /// Record that an agent has finished and, if all agents are done, complete the run.
    pub async fn notify_agent_completed(
        &self,
        trace_id: &str,
        agent_run_id: &str,
        success: bool,
        error: String,
    ) -> Result<(), Status> {
        let terminal_status = if success {
            AgentStatus::Completed
        } else {
            AgentStatus::Failed(error.clone())
        };

        self.runs
            .lock()
            .await
            .update_agent_status(agent_run_id, terminal_status)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let _ = self.run_tx.send(RunEvent {
            trace_id: trace_id.to_string(),
            event: Some(run_event::Event::AgentCompleted(AgentCompletedEvent {
                agent_run_id: agent_run_id.to_string(),
                success,
                error: error.clone(),
            })),
        });

        // If all agents are done, mark the run complete.
        let active = self
            .runs
            .lock()
            .await
            .active_agent_count(trace_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if active == 0 {
            let run_status = if success {
                RunStatus::Completed
            } else {
                RunStatus::Failed(error.clone())
            };
            self.runs
                .lock()
                .await
                .update_run_status(trace_id, run_status)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            let _ = self.run_tx.send(RunEvent {
                trace_id: trace_id.to_string(),
                event: Some(run_event::Event::RunCompleted(RunCompletedEvent {
                    success,
                    error,
                })),
            });
        }

        Ok(())
    }
}

/// Background task: bridges `TriggerFiredEvent`s from the batch writer into
/// `dispatch_agent` calls, but only for tuples that belong to active runs.
async fn trigger_bridge(
    mut rx: broadcast::Receiver<TriggerFiredEvent>,
    playbooks: Arc<Mutex<dyn PlaybookStore>>,
    runs: Arc<Mutex<dyn RunStore>>,
    run_tx: broadcast::Sender<RunEvent>,
    dispatch_tx: broadcast::Sender<AgentDispatchedEvent>,
) {
    loop {
        let event = match rx.recv().await {
            Ok(e) => e,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        };

        // Is there an active run for this (playbook, trace_id) pair?
        let run = match runs.lock().await.get_run(&event.trace_id).await {
            Ok(Some(r))
                if r.playbook_name == event.playbook
                    && matches!(r.status, RunStatus::Running) =>
            {
                r
            }
            _ => continue,
        };

        // Find the trigger in the playbook that dispatches this agent.
        let playbook = match playbooks.lock().await.get(&run.playbook_name).await {
            Ok(Some(p)) => p,
            _ => continue,
        };

        let trigger =
            match playbook.triggers.iter().find(|t| t.execution.agent == event.agent) {
                Some(t) => t.clone(),
                None => continue,
            };

        // Parse the tuple data payload.
        let tuple_data: serde_json::Value =
            match serde_json::from_str(&event.tuple_data) {
                Ok(v) => v,
                Err(_) => continue,
            };

        // Build a combined lookup: data fields + metadata fields from the event.
        let mut lookup = tuple_data.as_object().cloned().unwrap_or_default();
        lookup.insert(
            "uuid7".to_string(),
            serde_json::Value::String(event.tuple_uuid7.clone()),
        );
        lookup.insert(
            "type".to_string(),
            serde_json::Value::String(event.tuple_type.clone()),
        );
        lookup.insert(
            "trace_id".to_string(),
            serde_json::Value::String(event.trace_id.clone()),
        );
        let lookup = serde_json::Value::Object(lookup);

        // Resolve params using ParamSource.
        let mut resolved = serde_json::Map::new();
        let mut params_ok = true;
        for (param, source) in &trigger.execution.params {
            let value = match source {
                ParamSource::Literal(v) => v.clone(),
                ParamSource::Field(field) => match lookup.get(field) {
                    Some(v) => v.clone(),
                    None => {
                        params_ok = false;
                        break;
                    }
                },
            };
            resolved.insert(param.clone(), value);
        }
        if !params_ok {
            continue;
        }

        let params = serde_json::Value::Object(resolved);
        let id = Uuid::now_v7().to_string();
        let dispatched = AgentDispatchedEvent {
            agent_run_id: id.clone(),
            agent_name: trigger.execution.agent.clone(),
            params: params.to_string(),
        };

        if runs
            .lock()
            .await
            .create_agent_run(AgentRun {
                id,
                trace_id: event.trace_id.clone(),
                agent_name: trigger.execution.agent.clone(),
                triggered_by: Some(event.tuple_uuid7.clone()),
                params,
                status: AgentStatus::Dispatched,
                dispatched_at: Utc::now(),
            })
            .await
            .is_ok()
        {
            let run_event = RunEvent {
                trace_id: event.trace_id.clone(),
                event: Some(run_event::Event::AgentDispatched(dispatched.clone())),
            };
            let _ = run_tx.send(run_event);
            let _ = dispatch_tx.send(dispatched);
        }
    }
}
