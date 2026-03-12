#![deny(warnings)]

pub mod agent_store;
pub mod filter_store;
pub mod playbook_store;
pub mod run_store;
pub mod schema_store;
pub mod tuple_store;

pub use agent_store::{AgentStore, InMemoryAgentStore};
pub use filter_store::{FilterStore, InMemoryFilterStore};
pub use playbook_store::{InMemoryPlaybookStore, PlaybookStore};
pub use run_store::{InMemoryRunStore, RunStore};
pub use schema_store::{InMemorySchemaStore, SchemaStore};
pub use tuple_store::{BatchConfig, InMemoryTupleStore, TupleStore};

#[cfg(feature = "fdb")]
pub mod fdb_store;
#[cfg(feature = "fdb")]
pub use fdb_store::{
    FdbAgentStore, FdbFilterStore, FdbPlaybookStore, FdbRunStore, FdbSchemaStore, FdbTupleStore,
};

#[cfg(feature = "postgres")]
pub mod pg_store;
#[cfg(feature = "postgres")]
pub use pg_store::{
    PgAgentStore, PgFilterStore, PgPlaybookStore, PgRunStore, PgSchemaStore, PgTupleStore,
};

#[cfg(feature = "dynamodb")]
pub mod dynamo_store;
#[cfg(feature = "dynamodb")]
pub use dynamo_store::{
    ensure_tables as ensure_dynamo_tables, DynamoAgentStore, DynamoFilterStore,
    DynamoPlaybookStore, DynamoRunStore, DynamoSchemaStore, DynamoTupleStore,
};
