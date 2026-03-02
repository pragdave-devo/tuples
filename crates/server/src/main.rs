#![deny(warnings)]

mod batch_writer;
mod executor;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

use proto::tuples::{
    tuples_server::{Tuples, TuplesServer},
    AgentCompletedRequest, AgentLifecycleRequest, AgentResponse, Empty, FilterResponse,
    GetAgentRequest, GetPlaybookRequest, GetRunRequest, GetSchemaRequest, GetTupleRequest,
    ListAgentsResponse, ListFiltersResponse, ListPlaybooksResponse, ListRunsResponse,
    ListSchemasResponse, MatchTupleRequest, MatchTupleResponse, PlaybookResponse, PutTupleRequest,
    PutTupleResponse, RegisterAgentRequest, RegisterFilterRequest, RegisterPlaybookRequest,
    RegisterSchemaRequest, RunPlaybookRequest, RunPlaybookResponse, RunResponse, SchemaResponse,
    TriggerFiredEvent, TupleResponse, VersionResponse, WatchAgentDispatchRequest,
    WatchRunRequest, WatchTriggersRequest, AgentDispatchedEvent, RunEvent,
};
use batch_writer::BatchWriter;
use executor::Executor;
use tuples_core::{
    agent::Agent,
    filter::Filter,
    playbook::{ParamSource, Playbook},
    schema::Schema,
    tuple::Tuple,
};
use tuples_storage::{
    FilterStore, InMemoryAgentStore, InMemoryFilterStore, InMemoryPlaybookStore,
    InMemoryRunStore, InMemorySchemaStore, InMemoryTupleStore, PlaybookStore, RunStore,
    SchemaStore, TupleStore, AgentStore,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ADDR: &str = "[::1]:50051";
const BROADCAST_CAPACITY: usize = 256;

#[derive(Parser)]
struct Args {
    #[cfg(feature = "fdb")]
    #[arg(long)]
    fdb: bool,
}

struct TuplesService {
    schemas: Arc<Mutex<dyn SchemaStore>>,
    tuples: Arc<dyn TupleStore>,
    filters: Arc<Mutex<dyn FilterStore>>,
    playbooks: Arc<Mutex<dyn PlaybookStore>>,
    agents: Arc<Mutex<dyn AgentStore>>,
    runs: Arc<Mutex<dyn RunStore>>,
    trigger_tx: broadcast::Sender<TriggerFiredEvent>,
    batch_writer: Arc<BatchWriter>,
    executor: Arc<Executor>,
}

impl TuplesService {
    fn new() -> Self {
        let tuples: Arc<dyn TupleStore> = Arc::new(InMemoryTupleStore::default());
        let playbooks: Arc<Mutex<dyn PlaybookStore>> =
            Arc::new(Mutex::new(InMemoryPlaybookStore::default()));
        let agents: Arc<Mutex<dyn AgentStore>> =
            Arc::new(Mutex::new(InMemoryAgentStore::default()));
        let runs: Arc<Mutex<dyn RunStore>> = Arc::new(Mutex::new(InMemoryRunStore::default()));
        let schemas: Arc<Mutex<dyn SchemaStore>> =
            Arc::new(Mutex::new(InMemorySchemaStore::default()));

        let (trigger_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let batch_writer = Arc::new(BatchWriter::new(
            Arc::clone(&tuples),
            Arc::clone(&playbooks),
            trigger_tx.clone(),
        ));

        let (run_tx, _) = broadcast::channel::<RunEvent>(BROADCAST_CAPACITY);
        let (dispatch_tx, _) = broadcast::channel::<AgentDispatchedEvent>(BROADCAST_CAPACITY);

        let executor = Arc::new(Executor::new(
            Arc::clone(&playbooks),
            Arc::clone(&agents),
            Arc::clone(&schemas),
            Arc::clone(&runs),
            Arc::clone(&batch_writer),
            trigger_tx.clone(),
            run_tx,
            dispatch_tx,
        ));

        Self {
            schemas,
            tuples,
            filters: Arc::new(Mutex::new(InMemoryFilterStore::default())),
            playbooks,
            agents,
            runs,
            trigger_tx,
            batch_writer,
            executor,
        }
    }

    #[cfg(feature = "fdb")]
    async fn new_fdb(db: Arc<foundationdb::Database>) -> Result<Self> {
        use tuples_storage::{
            FdbFilterStore, FdbPlaybookStore, FdbSchemaStore, FdbTupleStore,
        };
        let filters = FdbFilterStore::load(db.clone()).await?;
        let fdb_playbooks = FdbPlaybookStore::load(db.clone()).await?;
        let tuples: Arc<dyn TupleStore> = Arc::new(FdbTupleStore::new(db.clone()));
        let playbooks: Arc<Mutex<dyn PlaybookStore>> =
            Arc::new(Mutex::new(fdb_playbooks));
        let agents: Arc<Mutex<dyn AgentStore>> =
            Arc::new(Mutex::new(InMemoryAgentStore::default()));
        let runs: Arc<Mutex<dyn RunStore>> =
            Arc::new(Mutex::new(InMemoryRunStore::default()));
        let schemas: Arc<Mutex<dyn SchemaStore>> =
            Arc::new(Mutex::new(FdbSchemaStore::new(db.clone())));

        let (trigger_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let batch_writer = Arc::new(BatchWriter::new(
            Arc::clone(&tuples),
            Arc::clone(&playbooks),
            trigger_tx.clone(),
        ));

        let (run_tx, _) = broadcast::channel::<RunEvent>(BROADCAST_CAPACITY);
        let (dispatch_tx, _) = broadcast::channel::<AgentDispatchedEvent>(BROADCAST_CAPACITY);

        let executor = Arc::new(Executor::new(
            Arc::clone(&playbooks),
            Arc::clone(&agents),
            Arc::clone(&schemas),
            Arc::clone(&runs),
            Arc::clone(&batch_writer),
            trigger_tx.clone(),
            run_tx,
            dispatch_tx,
        ));

        Ok(Self {
            schemas,
            tuples,
            filters: Arc::new(Mutex::new(filters)),
            playbooks,
            agents,
            runs,
            trigger_tx,
            batch_writer,
            executor,
        })
    }
}

#[tonic::async_trait]
impl Tuples for TuplesService {
    async fn get_version(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(VersionResponse { version: VERSION.to_string() }))
    }

    async fn register_schema(
        &self,
        request: Request<RegisterSchemaRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let definition = serde_json::from_str(&req.definition)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON: {e}")))?;
        let schema = Schema { name: req.name, definition };
        self.schemas
            .lock()
            .await
            .register(schema)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<SchemaResponse>, Status> {
        let name = request.into_inner().name;
        let schema = self
            .schemas
            .lock()
            .await
            .get(&name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("schema '{name}' not found")))?;
        Ok(Response::new(SchemaResponse {
            name: schema.name,
            definition: schema.definition.to_string(),
        }))
    }

    async fn list_schemas(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListSchemasResponse>, Status> {
        let schemas = self
            .schemas
            .lock()
            .await
            .list()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let schemas = schemas
            .into_iter()
            .map(|s| SchemaResponse { name: s.name, definition: s.definition.to_string() })
            .collect();
        Ok(Response::new(ListSchemasResponse { schemas }))
    }

    async fn put_tuple(
        &self,
        request: Request<PutTupleRequest>,
    ) -> Result<Response<PutTupleResponse>, Status> {
        let req = request.into_inner();
        let guaranteed = req.guaranteed_write;

        let data: serde_json::Value = serde_json::from_str(&req.data)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON: {e}")))?;

        let schema = self
            .schemas
            .lock()
            .await
            .get(&req.r#type)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("schema '{}' not found", req.r#type)))?;

        schema
            .validate(&data)
            .map_err(|e| Status::invalid_argument(format!("validation failed: {e}")))?;

        let uuid7 = Uuid::now_v7().to_string();
        let created_at = Utc::now();

        let tuple = Tuple {
            uuid7: uuid7.clone(),
            trace_id: req.trace_id,
            created_at,
            tuple_type: req.r#type,
            data,
        };

        self.batch_writer.put(tuple, guaranteed).await?;

        Ok(Response::new(PutTupleResponse { uuid7, created_at: created_at.to_rfc3339() }))
    }

    async fn get_tuple(
        &self,
        request: Request<GetTupleRequest>,
    ) -> Result<Response<TupleResponse>, Status> {
        let uuid7 = request.into_inner().uuid7;
        let tuple = self
            .tuples
            .get(&uuid7)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("tuple '{uuid7}' not found")))?;

        Ok(Response::new(TupleResponse {
            uuid7: tuple.uuid7,
            trace_id: tuple.trace_id,
            created_at: tuple.created_at.to_rfc3339(),
            r#type: tuple.tuple_type,
            data: tuple.data.to_string(),
        }))
    }

    async fn register_filter(
        &self,
        request: Request<RegisterFilterRequest>,
    ) -> Result<Response<Empty>, Status> {
        let filter: Filter = serde_json::from_str(&request.into_inner().definition)
            .map_err(|e| Status::invalid_argument(format!("invalid filter JSON: {e}")))?;
        self.filters
            .lock()
            .await
            .register(filter)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn list_filters(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListFiltersResponse>, Status> {
        let filters = self
            .filters
            .lock()
            .await
            .list()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let filters = filters
            .into_iter()
            .map(|f| FilterResponse {
                id: f.id.clone(),
                definition: serde_json::to_string(&f).unwrap(),
            })
            .collect();
        Ok(Response::new(ListFiltersResponse { filters }))
    }

    async fn match_tuple(
        &self,
        request: Request<MatchTupleRequest>,
    ) -> Result<Response<MatchTupleResponse>, Status> {
        let data: serde_json::Value = serde_json::from_str(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON: {e}")))?;
        let filter_ids = self
            .filters
            .lock()
            .await
            .match_data(&data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(MatchTupleResponse { filter_ids }))
    }

    async fn register_playbook(
        &self,
        request: Request<RegisterPlaybookRequest>,
    ) -> Result<Response<Empty>, Status> {
        let playbook: Playbook = serde_json::from_str(&request.into_inner().definition)
            .map_err(|e| Status::invalid_argument(format!("invalid playbook JSON: {e}")))?;

        // Validate triggers: tuple_type must be registered, field refs must be known.
        {
            let schemas = self.schemas.lock().await;
            for trigger in &playbook.triggers {
                let schema = schemas
                    .get(&trigger.match_.tuple_type)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| {
                        Status::invalid_argument(format!(
                            "trigger '{}': tuple_type '{}' is not a registered schema",
                            trigger.id, trigger.match_.tuple_type
                        ))
                    })?;

                // Collect known fields: schema properties + tuple metadata fields.
                let mut known: HashSet<String> =
                    ["uuid7", "trace_id", "created_at", "type"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                if let Some(props) = schema
                    .definition
                    .get("properties")
                    .and_then(|p| p.as_object())
                {
                    known.extend(props.keys().cloned());
                }

                // Validate filter.exact keys.
                for key in trigger.match_.filter.exact.keys() {
                    if !known.contains(key) {
                        return Err(Status::invalid_argument(format!(
                            "trigger '{}': filter.exact key '{}' is not in schema '{}'",
                            trigger.id, key, trigger.match_.tuple_type
                        )));
                    }
                }
                // Validate filter.wildcards.
                for key in &trigger.match_.filter.wildcards {
                    if !known.contains(key) {
                        return Err(Status::invalid_argument(format!(
                            "trigger '{}': filter.wildcards key '{}' is not in schema '{}'",
                            trigger.id, key, trigger.match_.tuple_type
                        )));
                    }
                }
                // Validate filter.predicates.
                for pred in &trigger.match_.filter.predicates {
                    if !known.contains(&pred.key) {
                        return Err(Status::invalid_argument(format!(
                            "trigger '{}': predicate key '{}' is not in schema '{}'",
                            trigger.id, pred.key, trigger.match_.tuple_type
                        )));
                    }
                }
                // Validate Field param sources.
                for (param, source) in &trigger.execution.params {
                    if let ParamSource::Field(field) = source {
                        if !known.contains(field) {
                            return Err(Status::invalid_argument(format!(
                                "trigger '{}': param '{}' references unknown field '{}' in schema '{}'",
                                trigger.id, param, field, trigger.match_.tuple_type
                            )));
                        }
                    }
                }
            }
        }

        self.playbooks
            .lock()
            .await
            .register(playbook)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn get_playbook(
        &self,
        request: Request<GetPlaybookRequest>,
    ) -> Result<Response<PlaybookResponse>, Status> {
        let name = request.into_inner().name;
        let playbook = self
            .playbooks
            .lock()
            .await
            .get(&name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("playbook '{name}' not found")))?;
        let definition = serde_json::to_string(&playbook)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PlaybookResponse { name: playbook.name, definition }))
    }

    async fn list_playbooks(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListPlaybooksResponse>, Status> {
        let playbooks = self
            .playbooks
            .lock()
            .await
            .list()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let playbooks = playbooks
            .into_iter()
            .map(|p| {
                let definition = serde_json::to_string(&p).unwrap_or_default();
                PlaybookResponse { name: p.name, definition }
            })
            .collect();
        Ok(Response::new(ListPlaybooksResponse { playbooks }))
    }

    type WatchTriggersStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<TriggerFiredEvent, Status>> + Send>>;

    async fn watch_triggers(
        &self,
        request: Request<WatchTriggersRequest>,
    ) -> Result<Response<Self::WatchTriggersStream>, Status> {
        let playbook_filter = request.into_inner().playbook;
        let rx = self.trigger_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(event) => {
                if playbook_filter.is_empty() || event.playbook == playbook_filter {
                    Some(Ok(event))
                } else {
                    None
                }
            }
            Err(_) => None, // lagged receiver — skip
        });
        Ok(Response::new(Box::pin(stream)))
    }

    // ── Agent management ────────────────────────────────────────────────────

    async fn register_agent(
        &self,
        request: Request<RegisterAgentRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        self.agents
            .lock()
            .await
            .register(Agent { name: req.name, description: req.description, schema: req.schema })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn get_agent(
        &self,
        request: Request<GetAgentRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let name = request.into_inner().name;
        let agent = self
            .agents
            .lock()
            .await
            .get(&name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("agent '{name}' not found")))?;
        Ok(Response::new(AgentResponse {
            name: agent.name,
            description: agent.description,
            schema: agent.schema,
        }))
    }

    async fn list_agents(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListAgentsResponse>, Status> {
        let agents = self
            .agents
            .lock()
            .await
            .list()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let agents = agents
            .into_iter()
            .map(|a| AgentResponse {
                name: a.name,
                description: a.description,
                schema: a.schema,
            })
            .collect();
        Ok(Response::new(ListAgentsResponse { agents }))
    }

    // ── Playbook execution ───────────────────────────────────────────────────

    async fn run_playbook(
        &self,
        request: Request<RunPlaybookRequest>,
    ) -> Result<Response<RunPlaybookResponse>, Status> {
        let req = request.into_inner();
        let params: serde_json::Value = serde_json::from_str(&req.params)
            .map_err(|e| Status::invalid_argument(format!("invalid params JSON: {e}")))?;
        let trace_id = self.executor.run_playbook(&req.playbook_name, params).await?;
        Ok(Response::new(RunPlaybookResponse { trace_id }))
    }

    async fn get_run(
        &self,
        request: Request<GetRunRequest>,
    ) -> Result<Response<RunResponse>, Status> {
        let trace_id = request.into_inner().trace_id;
        let run = self
            .runs
            .lock()
            .await
            .get_run(&trace_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("run '{trace_id}' not found")))?;
        Ok(Response::new(run_to_proto(run)))
    }

    async fn list_runs(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        let runs = self
            .runs
            .lock()
            .await
            .list_runs()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let runs = runs.into_iter().map(run_to_proto).collect();
        Ok(Response::new(ListRunsResponse { runs }))
    }

    // ── Agent lifecycle callbacks ────────────────────────────────────────────

    async fn notify_agent_started(
        &self,
        request: Request<AgentLifecycleRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        self.executor.notify_agent_started(&req.trace_id, &req.agent_run_id).await?;
        Ok(Response::new(Empty {}))
    }

    async fn notify_agent_completed(
        &self,
        request: Request<AgentCompletedRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        self.executor
            .notify_agent_completed(&req.trace_id, &req.agent_run_id, req.success, req.error)
            .await?;
        Ok(Response::new(Empty {}))
    }

    // ── Streaming subscriptions ─────────────────────────────────────────────

    type WatchRunStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<RunEvent, Status>> + Send>>;

    async fn watch_run(
        &self,
        request: Request<WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        let trace_id = request.into_inner().trace_id;
        let rx = self.executor.run_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(event) if event.trace_id == trace_id => Some(Ok(event)),
            Ok(_) => None,
            Err(_) => None, // lagged — skip
        });
        Ok(Response::new(Box::pin(stream)))
    }

    type WatchAgentDispatchStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<AgentDispatchedEvent, Status>> + Send>>;

    async fn watch_agent_dispatch(
        &self,
        request: Request<WatchAgentDispatchRequest>,
    ) -> Result<Response<Self::WatchAgentDispatchStream>, Status> {
        let agent_name = request.into_inner().agent_name;
        let rx = self.executor.dispatch_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(event) if event.agent_name == agent_name => Some(Ok(event)),
            Ok(_) => None,
            Err(_) => None, // lagged — skip
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn run_to_proto(run: tuples_core::run::PlaybookRun) -> RunResponse {
    let status = match &run.status {
        tuples_core::run::RunStatus::Running => "Running".to_string(),
        tuples_core::run::RunStatus::Completed => "Completed".to_string(),
        tuples_core::run::RunStatus::Failed(e) => format!("Failed: {e}"),
    };
    RunResponse {
        trace_id: run.trace_id,
        playbook_name: run.playbook_name,
        started_at: run.started_at.to_rfc3339(),
        status,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _args = Args::parse();
    let addr = ADDR.parse()?;

    // Network handle must outlive all FDB operations — keep it in scope until main() returns.
    #[cfg(feature = "fdb")]
    let _fdb_network = if _args.fdb { Some(unsafe { foundationdb::boot() }) } else { None };

    #[cfg(feature = "fdb")]
    let service = if _args.fdb {
        println!("tuplesd {VERSION} starting with FoundationDB backend");
        let db = Arc::new(foundationdb::Database::default()?);
        TuplesService::new_fdb(db).await?
    } else {
        TuplesService::new()
    };

    #[cfg(not(feature = "fdb"))]
    let service = TuplesService::new();

    println!("tuplesd {VERSION} listening on {addr}");

    Server::builder()
        .add_service(TuplesServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    println!("tuplesd stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn service() -> TuplesService {
        TuplesService::new()
    }

    async fn register_order_schema(svc: &TuplesService) {
        svc.register_schema(Request::new(RegisterSchemaRequest {
            name: "order".to_string(),
            definition: json!({
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            })
            .to_string(),
        }))
        .await
        .unwrap();
    }

    async fn register_processor_agent(svc: &TuplesService) {
        svc.register_agent(Request::new(RegisterAgentRequest {
            name: "processor".to_string(),
            description: "Processes orders".to_string(),
            schema: "order".to_string(),
        }))
        .await
        .unwrap();
    }

    async fn register_fulfil_playbook(svc: &TuplesService) {
        let playbook = json!({
            "name": "fulfil",
            "description": "Order fulfilment",
            "conductor": "processor",
            "agents": ["processor"],
            "triggers": [{
                "id": "t1",
                "match": { "tuple_type": "order" },
                "execution": {
                    "agent": "processor",
                    "params": { "order_id": { "field": "id" } }
                }
            }]
        });
        svc.register_playbook(Request::new(RegisterPlaybookRequest {
            definition: playbook.to_string(),
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn get_version_returns_package_version() {
        let svc = service();
        let resp = svc.get_version(Request::new(Empty {})).await.unwrap();
        assert_eq!(resp.into_inner().version, VERSION);
    }

    #[tokio::test]
    async fn register_and_get_schema() {
        let svc = service();
        register_order_schema(&svc).await;
        let resp = svc
            .get_schema(Request::new(GetSchemaRequest { name: "order".to_string() }))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().name, "order");
    }

    #[tokio::test]
    async fn get_schema_not_found() {
        let svc = service();
        let err = svc
            .get_schema(Request::new(GetSchemaRequest { name: "missing".to_string() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn list_schemas_empty() {
        let svc = service();
        let resp = svc.list_schemas(Request::new(Empty {})).await.unwrap();
        assert!(resp.into_inner().schemas.is_empty());
    }

    #[tokio::test]
    async fn register_schema_invalid_json() {
        let svc = service();
        let err = svc
            .register_schema(Request::new(RegisterSchemaRequest {
                name: "bad".to_string(),
                definition: "not json".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn put_and_get_tuple() {
        let svc = service();
        register_order_schema(&svc).await;

        let put_resp = svc
            .put_tuple(Request::new(PutTupleRequest {
                trace_id: "trace-1".to_string(),
                r#type: "order".to_string(),
                data: json!({ "id": "order-123" }).to_string(),
                guaranteed_write: true,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!put_resp.uuid7.is_empty());

        let get_resp = svc
            .get_tuple(Request::new(GetTupleRequest { uuid7: put_resp.uuid7.clone() }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(get_resp.uuid7, put_resp.uuid7);
        assert_eq!(get_resp.trace_id, "trace-1");
        assert_eq!(get_resp.r#type, "order");
    }

    #[tokio::test]
    async fn put_tuple_unknown_schema() {
        let svc = service();
        let err = svc
            .put_tuple(Request::new(PutTupleRequest {
                trace_id: "trace-1".to_string(),
                r#type: "unknown".to_string(),
                data: json!({}).to_string(),
                guaranteed_write: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn put_tuple_validation_failure() {
        let svc = service();
        register_order_schema(&svc).await;

        let err = svc
            .put_tuple(Request::new(PutTupleRequest {
                trace_id: "trace-1".to_string(),
                r#type: "order".to_string(),
                data: json!({ "id": 42 }).to_string(), // id must be string
                guaranteed_write: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_tuple_not_found() {
        let svc = service();
        let err = svc
            .get_tuple(Request::new(GetTupleRequest { uuid7: "nope".to_string() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn register_and_list_filter() {
        let svc = service();
        let def = json!({ "id": "f1", "exact": { "type": "order" } }).to_string();
        svc.register_filter(Request::new(RegisterFilterRequest { definition: def }))
            .await
            .unwrap();
        let resp = svc.list_filters(Request::new(Empty {})).await.unwrap().into_inner();
        assert_eq!(resp.filters.len(), 1);
        assert_eq!(resp.filters[0].id, "f1");
    }

    #[tokio::test]
    async fn match_tuple_returns_matching_filters() {
        let svc = service();
        svc.register_filter(Request::new(RegisterFilterRequest {
            definition: json!({ "id": "f-order", "exact": { "type": "order" } }).to_string(),
        }))
        .await
        .unwrap();
        svc.register_filter(Request::new(RegisterFilterRequest {
            definition: json!({ "id": "f-payment", "exact": { "type": "payment" } }).to_string(),
        }))
        .await
        .unwrap();

        let resp = svc
            .match_tuple(Request::new(MatchTupleRequest {
                data: json!({ "type": "order", "id": "o1" }).to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.filter_ids, vec!["f-order"]);
    }

    #[tokio::test]
    async fn register_filter_invalid_json() {
        let svc = service();
        let err = svc
            .register_filter(Request::new(RegisterFilterRequest {
                definition: "not json".to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn register_and_list_playbook() {
        let svc = service();
        // Schema must be registered before playbook (trigger validation).
        register_order_schema(&svc).await;
        register_fulfil_playbook(&svc).await;

        let resp = svc.list_playbooks(Request::new(Empty {})).await.unwrap().into_inner();
        assert_eq!(resp.playbooks.len(), 1);
        assert_eq!(resp.playbooks[0].name, "fulfil");
    }

    #[tokio::test]
    async fn register_playbook_unknown_schema_rejected() {
        let svc = service();
        // No schema registered — trigger validation should fail.
        let playbook = json!({
            "name": "fulfil",
            "description": "test",
            "conductor": "processor",
            "agents": ["processor"],
            "triggers": [{
                "id": "t1",
                "match": { "tuple_type": "order" },
                "execution": { "agent": "processor", "params": {} }
            }]
        });
        let err = svc
            .register_playbook(Request::new(RegisterPlaybookRequest {
                definition: playbook.to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn register_playbook_unknown_field_ref_rejected() {
        let svc = service();
        register_order_schema(&svc).await;
        let playbook = json!({
            "name": "fulfil",
            "description": "test",
            "conductor": "processor",
            "agents": ["processor"],
            "triggers": [{
                "id": "t1",
                "match": { "tuple_type": "order" },
                "execution": {
                    "agent": "processor",
                    "params": { "x": { "field": "nonexistent_field" } }
                }
            }]
        });
        let err = svc
            .register_playbook(Request::new(RegisterPlaybookRequest {
                definition: playbook.to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn put_tuple_fires_trigger_event() {
        let svc = service();
        register_order_schema(&svc).await;
        register_fulfil_playbook(&svc).await;

        // Subscribe before putting the tuple.
        let mut rx = svc.trigger_tx.subscribe();

        svc.put_tuple(Request::new(PutTupleRequest {
            trace_id: "trace-1".to_string(),
            r#type: "order".to_string(),
            data: json!({ "id": "order-123" }).to_string(),
            guaranteed_write: true,
        }))
        .await
        .unwrap();

        let event = rx.try_recv().expect("expected a TriggerFiredEvent");
        assert_eq!(event.playbook, "fulfil");
        assert_eq!(event.agent, "processor");
        assert_eq!(event.tuple_type, "order");
        assert_eq!(event.trace_id, "trace-1");
        assert_eq!(event.mapped_params.get("order_id"), Some(&"order-123".to_string()));
    }

    #[tokio::test]
    async fn put_tuple_no_match_no_event() {
        let svc = service();
        register_order_schema(&svc).await;
        register_fulfil_playbook(&svc).await;

        // Register a payment schema for the non-matching put.
        svc.register_schema(Request::new(RegisterSchemaRequest {
            name: "payment".to_string(),
            definition: json!({
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            })
            .to_string(),
        }))
        .await
        .unwrap();

        let mut rx = svc.trigger_tx.subscribe();

        svc.put_tuple(Request::new(PutTupleRequest {
            trace_id: "trace-1".to_string(),
            r#type: "payment".to_string(),
            data: json!({ "id": "pay-1" }).to_string(),
            guaranteed_write: true,
        }))
        .await
        .unwrap();

        assert!(rx.try_recv().is_err(), "no event should fire for non-matching tuple");
    }

    // ── Agent management tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn register_and_get_agent() {
        let svc = service();
        register_order_schema(&svc).await;
        register_processor_agent(&svc).await;
        let resp = svc
            .get_agent(Request::new(GetAgentRequest { name: "processor".to_string() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.name, "processor");
        assert_eq!(resp.schema, "order");
    }

    #[tokio::test]
    async fn get_agent_not_found() {
        let svc = service();
        let err = svc
            .get_agent(Request::new(GetAgentRequest { name: "missing".to_string() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn list_agents_empty() {
        let svc = service();
        let resp = svc.list_agents(Request::new(Empty {})).await.unwrap().into_inner();
        assert!(resp.agents.is_empty());
    }

    // ── Run lifecycle tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_playbook_returns_trace_id() {
        let svc = service();
        register_order_schema(&svc).await;
        register_processor_agent(&svc).await;
        register_fulfil_playbook(&svc).await;

        let resp = svc
            .run_playbook(Request::new(RunPlaybookRequest {
                playbook_name: "fulfil".to_string(),
                params: json!({ "id": "order-1" }).to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.trace_id.is_empty());
    }

    #[tokio::test]
    async fn run_playbook_not_found() {
        let svc = service();
        let err = svc
            .run_playbook(Request::new(RunPlaybookRequest {
                playbook_name: "missing".to_string(),
                params: json!({}).to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn run_playbook_invalid_params_rejected() {
        let svc = service();
        register_order_schema(&svc).await;
        register_processor_agent(&svc).await;
        register_fulfil_playbook(&svc).await;

        // id must be a string, not a number.
        let err = svc
            .run_playbook(Request::new(RunPlaybookRequest {
                playbook_name: "fulfil".to_string(),
                params: json!({ "id": 42 }).to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_run_not_found() {
        let svc = service();
        let err = svc
            .get_run(Request::new(GetRunRequest { trace_id: "missing".to_string() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn run_playbook_creates_run_record() {
        let svc = service();
        register_order_schema(&svc).await;
        register_processor_agent(&svc).await;
        register_fulfil_playbook(&svc).await;

        let trace_id = svc
            .run_playbook(Request::new(RunPlaybookRequest {
                playbook_name: "fulfil".to_string(),
                params: json!({ "id": "order-1" }).to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .trace_id;

        let run = svc
            .get_run(Request::new(GetRunRequest { trace_id: trace_id.clone() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(run.trace_id, trace_id);
        assert_eq!(run.playbook_name, "fulfil");
        assert_eq!(run.status, "Running");
    }

    #[tokio::test]
    async fn notify_agent_completed_closes_run() {
        let svc = service();
        register_order_schema(&svc).await;
        register_processor_agent(&svc).await;
        register_fulfil_playbook(&svc).await;

        // Start a run — this dispatches the conductor.
        let trace_id = svc
            .run_playbook(Request::new(RunPlaybookRequest {
                playbook_name: "fulfil".to_string(),
                params: json!({ "id": "order-1" }).to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
            .trace_id;

        // Find the dispatched agent run.
        let agent_runs = svc
            .runs
            .lock()
            .await
            .list_agent_runs(&trace_id)
            .await
            .unwrap();
        assert_eq!(agent_runs.len(), 1);
        let agent_run_id = agent_runs[0].id.clone();

        // Complete the agent.
        svc.notify_agent_completed(Request::new(AgentCompletedRequest {
            trace_id: trace_id.clone(),
            agent_run_id: agent_run_id.clone(),
            success: true,
            error: String::new(),
        }))
        .await
        .unwrap();

        // Run should now be Completed.
        let run = svc
            .get_run(Request::new(GetRunRequest { trace_id: trace_id.clone() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(run.status, "Completed");
    }
}
