#![deny(warnings)]

mod batch_writer;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use proto::tuples::{
    tuples_server::{Tuples, TuplesServer},
    Empty, FilterResponse, GetPlaybookRequest, GetSchemaRequest, GetTupleRequest,
    ListFiltersResponse, ListPlaybooksResponse, ListSchemasResponse, MatchTupleRequest,
    MatchTupleResponse, PlaybookResponse, PutTupleRequest, PutTupleResponse,
    RegisterFilterRequest, RegisterPlaybookRequest, RegisterSchemaRequest, SchemaResponse,
    TriggerFiredEvent, TupleResponse, VersionResponse, WatchTriggersRequest,
};
use batch_writer::BatchWriter;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Request, Response, Status};
use tuples_core::{filter::Filter, playbook::Playbook, schema::Schema, tuple::Tuple};
use tuples_storage::{
    FilterStore, InMemoryFilterStore, InMemoryPlaybookStore, InMemorySchemaStore,
    InMemoryTupleStore, PlaybookStore, SchemaStore, TupleStore,
};
use uuid::Uuid;

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
    tuples: Arc<Mutex<dyn TupleStore>>,
    filters: Arc<Mutex<dyn FilterStore>>,
    playbooks: Arc<Mutex<dyn PlaybookStore>>,
    trigger_tx: broadcast::Sender<TriggerFiredEvent>,
    batch_writer: BatchWriter,
}

impl TuplesService {
    fn new() -> Self {
        let tuples: Arc<Mutex<dyn TupleStore>> =
            Arc::new(Mutex::new(InMemoryTupleStore::default()));
        let playbooks: Arc<Mutex<dyn PlaybookStore>> =
            Arc::new(Mutex::new(InMemoryPlaybookStore::default()));
        let (trigger_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let batch_writer =
            BatchWriter::new(Arc::clone(&tuples), Arc::clone(&playbooks), trigger_tx.clone());
        Self {
            schemas: Arc::new(Mutex::new(InMemorySchemaStore::default())),
            tuples,
            filters: Arc::new(Mutex::new(InMemoryFilterStore::default())),
            playbooks,
            trigger_tx,
            batch_writer,
        }
    }

    #[cfg(feature = "fdb")]
    async fn new_fdb(db: Arc<foundationdb::Database>) -> Result<Self> {
        use tuples_storage::{FdbFilterStore, FdbPlaybookStore, FdbSchemaStore, FdbTupleStore};
        let filters = FdbFilterStore::load(db.clone()).await?;
        let fdb_playbooks = FdbPlaybookStore::load(db.clone()).await?;
        let tuples: Arc<Mutex<dyn TupleStore>> =
            Arc::new(Mutex::new(FdbTupleStore::new(db.clone())));
        let playbooks: Arc<Mutex<dyn PlaybookStore>> =
            Arc::new(Mutex::new(fdb_playbooks));
        let (trigger_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let batch_writer =
            BatchWriter::new(Arc::clone(&tuples), Arc::clone(&playbooks), trigger_tx.clone());
        Ok(Self {
            schemas: Arc::new(Mutex::new(FdbSchemaStore::new(db.clone()))),
            tuples,
            filters: Arc::new(Mutex::new(filters)),
            playbooks,
            trigger_tx,
            batch_writer,
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

        let tuple = Tuple { uuid7: uuid7.clone(), trace_id: req.trace_id, created_at, tuple_type: req.r#type, data };

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
            .lock()
            .await
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

    async fn register_fulfil_playbook(svc: &TuplesService) {
        let playbook = json!({
            "name": "fulfil",
            "description": "Order fulfilment",
            "conductor": "processor",
            "agents": [{"id": "processor", "description": "", "schema": "order"}],
            "triggers": [{
                "filter": {"id": "t1", "exact": {"type": "order"}, "wildcards": [], "predicates": []},
                "agent": "processor",
                "mapping": {"order_id": "id"}
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
        register_fulfil_playbook(&svc).await;

        let resp = svc.list_playbooks(Request::new(Empty {})).await.unwrap().into_inner();
        assert_eq!(resp.playbooks.len(), 1);
        assert_eq!(resp.playbooks[0].name, "fulfil");
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
}
